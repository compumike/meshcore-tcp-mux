#!/usr/bin/env python3
"""Read-only live upstream disconnect/reconnect gate through a local byte relay.

The physical companion must have no existing mux, CLI, Home Assistant, BLE, or
USB command producer. This script deliberately drops the mux-owned upstream TCP
connection; it does not reboot the node or send radio/configuration commands
beyond the mux's normal startup synchronization.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from collections import deque
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import socket
import sys
from typing import Any

from meshcore import EventType, MeshCore

from check_clients import connect, query_read_only


class RelayConnection:
    def __init__(
        self,
        generation: int,
        mux_writer: asyncio.StreamWriter,
        physical_writer: asyncio.StreamWriter,
    ) -> None:
        self.generation = generation
        self.mux_writer = mux_writer
        self.physical_writer = physical_writer

    async def close(self) -> None:
        for writer in (self.mux_writer, self.physical_writer):
            writer.close()
        await asyncio.gather(
            self.mux_writer.wait_closed(),
            self.physical_writer.wait_closed(),
            return_exceptions=True,
        )


class ExclusiveByteRelay:
    """Allows at most one relay-owned physical TCP connection at a time."""

    def __init__(self, physical_host: str, physical_port: int) -> None:
        self.physical_host = physical_host
        self.physical_port = physical_port
        self.server: asyncio.Server | None = None
        self.active: RelayConnection | None = None
        self.connections: asyncio.Queue[int | Exception] = asyncio.Queue()
        self.closed: asyncio.Queue[int] = asyncio.Queue()
        self._physical_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[None]] = set()
        self._generation = 0
        self._closing = False

    @property
    def port(self) -> int:
        if not self.server or not self.server.sockets:
            raise AssertionError("relay is not listening")
        return int(self.server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        self.server = await asyncio.start_server(
            self._accept, "127.0.0.1", 0
        )

    def _accept(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.create_task(self._handle(reader, writer))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _handle(
        self, mux_reader: asyncio.StreamReader, mux_writer: asyncio.StreamWriter
    ) -> None:
        async with self._physical_lock:
            if self._closing:
                mux_writer.close()
                await mux_writer.wait_closed()
                return

            try:
                physical_reader, physical_writer = await asyncio.open_connection(
                    self.physical_host, self.physical_port
                )
            except Exception as exc:
                mux_writer.close()
                await mux_writer.wait_closed()
                await self.connections.put(exc)
                return

            self._generation += 1
            connection = RelayConnection(
                self._generation, mux_writer, physical_writer
            )
            self.active = connection
            await self.connections.put(connection.generation)
            pumps = {
                asyncio.create_task(self._pump(mux_reader, physical_writer)),
                asyncio.create_task(self._pump(physical_reader, mux_writer)),
            }
            try:
                done, pending = await asyncio.wait(
                    pumps, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*done, *pending, return_exceptions=True)
            finally:
                await connection.close()
                if self.active is connection:
                    self.active = None
                await self.closed.put(connection.generation)

    async def _pump(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        while data := await reader.read(64 * 1024):
            writer.write(data)
            await writer.drain()

    async def drop(self, generation: int) -> None:
        connection = self.active
        if not connection or connection.generation != generation:
            actual = None if connection is None else connection.generation
            raise AssertionError(
                f"cannot drop relay generation {generation}; active={actual}"
            )
        await connection.close()
        observed = await asyncio.wait_for(self.closed.get(), timeout=3)
        if observed != generation:
            raise AssertionError(
                f"expected relay generation {generation} to close, got {observed}"
            )

    async def close(self) -> None:
        self._closing = True
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        if self.active:
            await self.active.close()
        if self._tasks:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)


def reserve_port() -> int:
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


async def read_mux_stderr(
    stream: asyncio.StreamReader,
    ready: asyncio.Queue[int],
    tail: deque[str],
) -> None:
    generation = 0
    while line := await stream.readline():
        text = line.decode(errors="replace").rstrip()
        tail.append(text)
        if " ready profile=native_v13 " in text:
            generation += 1
            await ready.put(generation)


async def expect_generation(
    queue: asyncio.Queue[int | Exception], expected: int
) -> None:
    observed = await asyncio.wait_for(queue.get(), timeout=8)
    if isinstance(observed, Exception):
        raise AssertionError(f"physical relay connection failed: {observed}")
    if observed != expected:
        raise AssertionError(
            f"expected generation {expected}, observed {observed}"
        )


async def query_pair(
    clients: tuple[MeshCore, MeshCore], generation: int
) -> list[dict[str, Any]]:
    results = await asyncio.gather(
        query_read_only(clients[0], f"generation-{generation}-a", 0),
        query_read_only(clients[1], f"generation-{generation}-b", 0),
    )
    if results[0]["contacts"] != results[1]["contacts"]:
        raise AssertionError(
            f"generation {generation}: clients observed different contact counts"
        )
    return results


def subscribe_disconnect(client: MeshCore) -> asyncio.Queue[bool]:
    observed: asyncio.Queue[bool] = asyncio.Queue(maxsize=1)

    def callback(_event: Any) -> None:
        if observed.empty():
            observed.put_nowait(True)

    client.subscribe(EventType.DISCONNECTED, callback)
    return observed


async def run(args: argparse.Namespace) -> dict[str, Any]:
    binary = Path(args.mux_binary).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise AssertionError(
            f"mux binary is not executable: {binary} (run make first)"
        )

    print(
        "NOTICE: this gate requires every existing mux/direct upstream client "
        "to be stopped before it takes exclusive physical TCP ownership.",
        file=sys.stderr,
        flush=True,
    )

    relay = ExclusiveByteRelay(args.physical_host, args.physical_port)
    process: asyncio.subprocess.Process | None = None
    stderr_task: asyncio.Task[None] | None = None
    ready: asyncio.Queue[int] = asyncio.Queue()
    stderr_tail: deque[str] = deque(maxlen=40)
    clients: list[MeshCore] = []
    try:
        await relay.start()
        listen_port = reserve_port()
        process = await asyncio.create_subprocess_exec(
            str(binary),
            "--upstream-host",
            "127.0.0.1",
            "--upstream-port",
            str(relay.port),
            "--listen-host",
            "127.0.0.1",
            "--listen-port",
            str(listen_port),
            "--poll-interval",
            "60",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        if process.stderr is None:
            raise AssertionError("mux stderr pipe was not created")
        stderr_task = asyncio.create_task(
            read_mux_stderr(process.stderr, ready, stderr_tail)
        )

        await expect_generation(relay.connections, 1)
        await expect_generation(ready, 1)
        old = await asyncio.gather(
            connect("127.0.0.1", listen_port, args.timeout, "generation-1-a"),
            connect("127.0.0.1", listen_port, args.timeout, "generation-1-b"),
        )
        clients.extend(old)
        old_pair = (old[0], old[1])
        old_results = await query_pair(old_pair, 1)
        if old_pair[0].self_info["public_key"] != old_pair[1].self_info["public_key"]:
            raise AssertionError("generation 1 clients observed different identities")

        disconnected = tuple(subscribe_disconnect(client) for client in old_pair)
        await relay.drop(1)
        await asyncio.wait_for(
            asyncio.gather(*(queue.get() for queue in disconnected)),
            timeout=args.timeout,
        )

        await expect_generation(relay.connections, 2)
        await expect_generation(ready, 2)
        fresh = await asyncio.gather(
            connect("127.0.0.1", listen_port, args.timeout, "generation-2-a"),
            connect("127.0.0.1", listen_port, args.timeout, "generation-2-b"),
        )
        clients.extend(fresh)
        fresh_pair = (fresh[0], fresh[1])
        fresh_results = await query_pair(fresh_pair, 2)
        identities = {
            old_pair[0].self_info["public_key"],
            old_pair[1].self_info["public_key"],
            fresh_pair[0].self_info["public_key"],
            fresh_pair[1].self_info["public_key"],
        }
        if len(identities) != 1:
            raise AssertionError("physical identity changed across reconnect gate")

        return {
            "status": "ok",
            "generations": 2,
            "physical_connections": 2,
            "old_sessions_closed": 2,
            "fresh_sessions": 2,
            "read_only_query_sets": 4,
            "contact_count_generation_1": old_results[0]["contacts"],
            "contact_count_generation_2": fresh_results[0]["contacts"],
            "meshcore_py": importlib.metadata.version("meshcore"),
        }
    except Exception as exc:
        detail = str(exc)
        if stderr_tail:
            detail += "\nmux stderr tail:\n" + "\n".join(stderr_tail)
        raise AssertionError(detail) from exc
    finally:
        await asyncio.gather(
            *(client.disconnect() for client in clients),
            return_exceptions=True,
        )
        if process and process.returncode is None:
            process.send_signal(signal.SIGTERM)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(process.wait(), timeout=5)
        if process and process.returncode is None:
            process.kill()
            await process.wait()
        await relay.close()
        if stderr_task:
            await stderr_task


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--physical-host", required=True)
    parser.add_argument("--physical-port", required=True, type=int)
    parser.add_argument("--mux-binary", default="out/meshcore-tcp-mux")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    if not 1 <= args.physical_port <= 65535:
        parser.error("--physical-port must be between 1 and 65535")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def main() -> int:
    try:
        result = asyncio.run(run(parse_args()))
    except Exception as exc:
        print(f"check_live_reconnect: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
