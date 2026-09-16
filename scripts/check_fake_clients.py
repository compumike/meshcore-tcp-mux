#!/usr/bin/env python3
"""End-to-end mux check with a stdlib fake companion and real meshcore_py clients."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import signal
import socket
import sys
from collections import Counter, deque
from pathlib import Path
from typing import Any

from meshcore import EventType, MeshCore


def frame(payload: bytes) -> bytes:
    return b">" + len(payload).to_bytes(2, "little") + payload


def self_info() -> bytes:
    # SELF_INFO (5): radio type/power/max power at 1..3; synthetic public key
    # at 4..35; signed LE coordinates at 36..43; options at 44..47; LE radio
    # frequency/bandwidth at 48..55; SF/CR at 56..57, then the fake node name.
    key = bytes(range(32))
    fixed = (
        bytes([5, 1, 10, 22])
        + key
        + (0).to_bytes(4, "little", signed=True) * 2
        + bytes([1, 0, 0, 0])
        + (915_000).to_bytes(4, "little")
        + (250_000).to_bytes(4, "little")
        + bytes([9, 5])
    )
    assert len(fixed) == 58
    return fixed + b"fake-companion"


def device_info() -> bytes:
    # DEVICE_INFO (13): protocol 13, contact/channel capacities, synthetic PIN
    # (u32 LE) at 4..7, then NUL-padded build/model/version fields at 8/20/60.
    # Two final capability bytes make 82 payload bytes, excluding the envelope.
    def field(value: bytes, size: int) -> bytes:
        return value[: size - 1].ljust(size, b"\0")

    payload = (
        bytes([13, 13, 16, 8])
        + (123456).to_bytes(4, "little")
        + field(b"2026-09-13", 12)
        + field(b"stdlib-fake", 40)
        + field(b"v1.17.1-fake", 20)
        + bytes([0, 0])
    )
    assert len(payload) == 82
    return payload


def contact_record(seed: int) -> bytes:
    # CONTACT (3): synthetic key at 1..32, type/flags/path length at 33..35,
    # fixed 64-byte path storage at 36..99 and 32-byte name at 100..131.
    # LE advert time, signed latitude/longitude, and lastmod occupy 132..147.
    # The seed distinguishes contacts; zero path length means no path hashes.
    name = f"fake-{seed}".encode().ljust(32, b"\0")
    payload = (
        bytes([3])
        + bytes([seed]) * 32
        + bytes([1, 0, 0])
        + bytes(64)
        + name
        + (100 + seed).to_bytes(4, "little")
        + (0).to_bytes(4, "little", signed=True) * 2
        + (200 + seed).to_bytes(4, "little")
    )
    assert len(payload) == 148
    return payload


def contact_message(text: str, timestamp: int = 1234) -> bytes:
    # V3: code, SNR, two reserved bytes, peer prefix, path, text type,
    # timestamp, then the body. The mux must remove only bytes 1..3 for legacy.
    return (
        bytes([0x10, 8, 0, 0])
        + b"PEER01"
        + bytes([0xFF, 0])
        + timestamp.to_bytes(4, "little")
        + text.encode()
    )


def legacy_contact_message(text: str, timestamp: int = 1111) -> bytes:
    # CONTACT_MESSAGE (0x07): peer prefix, unknown path (0xff), plain text (0), timestamp, body.
    return (
        bytes([0x07])
        + b"OLD001"
        + bytes([0xFF, 0])
        + timestamp.to_bytes(4, "little")
        + text.encode()
    )


def channel_message(text: str, timestamp: int = 2345) -> bytes:
    # CHANNEL_MESSAGE_V3 (0x11): SNR, two reserved bytes, channel 1, unknown path (0xff), plain text (0).
    return (
        bytes([0x11, 12, 0, 0, 1, 0xFF, 0])
        + timestamp.to_bytes(4, "little")
        + text.encode()
    )


class FakeCompanion:
    # Loopback companion implementing startup, contacts, time, and inbox operations for real Python clients.
    def __init__(self) -> None:
        self.server: asyncio.Server | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.write_lock = asyncio.Lock()
        self.offline: deque[bytes] = deque()
        self.commands: Counter[int] = Counter()
        self.query_targets: list[int] = []
        self.time_value = 1_700_000_000
        self.connected = asyncio.Event()

    @property
    def port(self) -> int:
        assert self.server and self.server.sockets
        return int(self.server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)

    async def close(self) -> None:
        if self.writer:
            self.writer.close()
            with contextlib.suppress(Exception):
                await self.writer.wait_closed()
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def send(self, payload: bytes) -> None:
        writer = self.writer
        if not writer:
            raise AssertionError("fake companion has no upstream connection")
        async with self.write_lock:
            writer.write(frame(payload))
            await writer.drain()

    async def enqueue(self, *payloads: bytes) -> None:
        self.offline.extend(payloads)
        await self.send(bytes([0x83]))  # MSG_WAITING: prompt the mux to drain the physical inbox.

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if self.writer and not self.writer.is_closing():
            self.writer.close()
        self.writer = writer
        self.connected.set()
        try:
            while True:
                header = await reader.readexactly(3)
                if header[0] != 0x3C:  # '<': client-to-companion TCP direction marker.
                    raise AssertionError(f"wrong request marker 0x{header[0]:02x}")
                length = int.from_bytes(header[1:3], "little")
                payload = await reader.readexactly(length)
                await self.command(payload)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            if self.writer is writer:
                self.writer = None
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def command(self, payload: bytes) -> None:
        if not payload:
            raise AssertionError("mux sent an empty command")
        opcode = payload[0]
        self.commands[opcode] += 1
        if opcode == 1:  # APP_START.
            await self.send(self_info())
        elif opcode == 22:  # DEVICE_QUERY.
            if len(payload) < 2:
                raise AssertionError("truncated DEVICE_QUERY")
            self.query_targets.append(payload[1])
            await self.send(device_info())
        elif opcode == 54:  # SET_FLOOD_SCOPE_KEY.
            await self.send(bytes([0]))  # OK: scope accepted.
        elif opcode == 10:  # SYNC_NEXT_MESSAGE.
            await self.send(self.offline.popleft() if self.offline else bytes([10]))  # NO_MORE_MESSAGES.
        elif opcode == 4:  # GET_CONTACTS.
            # meshcore_py get_contacts installs stream listeners immediately after
            # its fire-and-return send. Yield so even an in-process fake cannot
            # outrun that public API sequence.
            await asyncio.sleep(0.02)
            await self.send(bytes([2]) + (2).to_bytes(4, "little"))  # CONTACTS_START: two records follow.
            await self.send(contact_record(1))
            await self.send(contact_record(2))
            await self.send(bytes([4]) + (202).to_bytes(4, "little"))  # END_OF_CONTACTS: synthetic last-modified time.
        elif opcode == 5:  # GET_DEVICE_TIME.
            self.time_value += 1
            await self.send(bytes([9]) + self.time_value.to_bytes(4, "little"))  # CURRENT_TIME: u32 timestamp.
        else:
            await self.send(bytes([1, 1]))  # ERR, UNSUPPORTED_CMD: fake intentionally implements a subset.


def reserve_port() -> int:
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def require_event(event: Any, expected: EventType, operation: str) -> Any:
    if event is None or event.type != expected:
        actual = None if event is None else (event.type.value, event.payload)
        raise AssertionError(f"{operation}: expected {expected.value}, got {actual!r}")
    return event


async def connect(port: int, label: str) -> MeshCore:
    client = await MeshCore.create_tcp(
        "127.0.0.1", port, only_error=True, default_timeout=4, auto_reconnect=False
    )
    if client is None:
        raise AssertionError(f"{label}: APP_START failed")
    return client


async def wait_until(predicate: Any, timeout: float, description: str) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"timed out waiting for {description}")
        await asyncio.sleep(0.01)


async def drain_three(client: MeshCore, label: str) -> list[Any]:
    events = []
    for index in range(3):
        event = await client.commands.get_msg(timeout=4)
        if event.type not in (EventType.CONTACT_MSG_RECV, EventType.CHANNEL_MSG_RECV):
            raise AssertionError(
                f"{label} inbox {index}: unexpected {event.type.value} {event.payload!r}"
            )
        events.append(event)
    return events


async def read_times(client: MeshCore, label: str) -> list[int]:
    values = []
    for index in range(3):
        event = require_event(
            await client.commands.get_time(),
            EventType.CURRENT_TIME,
            f"{label} time {index}",
        )
        values.append(event.payload["time"])
    return values


async def stderr_reader(
    stream: asyncio.StreamReader, ready: asyncio.Event, tail: deque[str]
) -> None:
    while line := await stream.readline():
        text = line.decode(errors="replace").rstrip()
        tail.append(text)
        # Runtime readiness is an INFO event; stdout remains reserved for the
        # smoke check's machine-readable result.
        if "event=upstream.ready " in text:
            ready.set()


async def run(args: argparse.Namespace) -> dict[str, Any]:
    binary = Path(args.mux_binary).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise AssertionError(f"mux binary is not executable: {binary} (run make first)")

    fake = FakeCompanion()
    # This entry predates startup and the node-global target-13 normalization.
    # With no downstream session, the mux must leave it in physical backlog.
    fake.offline.append(legacy_contact_message("native-legacy-backlog"))
    await fake.start()
    listen_port = reserve_port()
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "--upstream-host",
        "127.0.0.1",
        "--upstream-port",
        str(fake.port),
        "--listen-host",
        "127.0.0.1",
        "--listen-multi-client-port",
        str(listen_port),
        "--poll-interval",
        "60",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stderr
    ready = asyncio.Event()
    stderr_tail: deque[str] = deque(maxlen=30)
    stderr_task = asyncio.create_task(stderr_reader(process.stderr, ready, stderr_tail))
    clients: list[MeshCore] = []
    try:
        await asyncio.wait_for(ready.wait(), timeout=6)

        backlog_probe = await connect(listen_port, "modern-backlog-probe")
        clients.append(backlog_probe)
        require_event(
            await backlog_probe.commands.send_device_query(),
            EventType.DEVICE_INFO,
            "modern backlog DEVICE_QUERY",
        )
        backlog = require_event(
            await backlog_probe.commands.get_msg(timeout=4),
            EventType.CONTACT_MSG_RECV,
            "native legacy backlog",
        )
        if backlog.payload["text"] != "native-legacy-backlog":
            raise AssertionError("native legacy backlog body changed")
        if "SNR" in backlog.payload:
            raise AssertionError("mux invented SNR while upgrading native legacy backlog")
        await backlog_probe.disconnect()
        clients.remove(backlog_probe)

        legacy, modern = await asyncio.gather(
            connect(listen_port, "legacy"), connect(listen_port, "modern")
        )
        clients.extend((legacy, modern))

        legacy_query = require_event(
            await legacy.commands.send(
                b"\x16\x00", [EventType.DEVICE_INFO, EventType.ERROR]
            ),
            EventType.DEVICE_INFO,
            "legacy DEVICE_QUERY",
        )
        modern_query = require_event(
            await modern.commands.send_device_query(),
            EventType.DEVICE_INFO,
            "modern DEVICE_QUERY",
        )
        if legacy_query.payload["fw ver"] != 13 or modern_query.payload["fw ver"] != 13:
            raise AssertionError("clients did not receive real protocol level 13")

        contacts = await asyncio.gather(
            legacy.commands.get_contacts(timeout=4),
            modern.commands.get_contacts(timeout=4),
        )
        for label, event in zip(("legacy", "modern"), contacts):
            event = require_event(event, EventType.CONTACTS, f"{label} contacts")
            if len(event.payload) != 2:
                raise AssertionError(f"{label}: expected two fake contacts")

        time_runs = await asyncio.gather(
            read_times(legacy, "legacy"), read_times(modern, "modern")
        )
        times = time_runs[0] + time_runs[1]
        if len(set(times)) != len(times):
            raise AssertionError("generic CURRENT_TIME responses were duplicated/misowned")

        baseline_pops = fake.commands[10]
        duplicate = contact_message("duplicate")
        await fake.enqueue(duplicate, duplicate, channel_message("channel"))
        await wait_until(
            lambda: not fake.offline and fake.commands[10] >= baseline_pops + 3,
            4,
            "mux inbox pump",
        )
        legacy_events, modern_events = await asyncio.gather(
            drain_three(legacy, "legacy"), drain_three(modern, "modern")
        )
        for label, events in (("legacy", legacy_events), ("modern", modern_events)):
            if [event.payload["text"] for event in events] != [
                "duplicate",
                "duplicate",
                "channel",
            ]:
                raise AssertionError(f"{label}: duplicate/body/order was not retained")
        if "SNR" in legacy_events[0].payload or "SNR" in legacy_events[2].payload:
            raise AssertionError("legacy session received V3 text headers")
        if "SNR" not in modern_events[0].payload or "SNR" not in modern_events[2].payload:
            raise AssertionError("modern session lost V3 text headers")

        # Fan out an item before the replacement joins. It must remain only in
        # the already-present modern session, not become reconnect history.
        await legacy.disconnect()
        clients.remove(legacy)
        old_item = contact_message("before-reconnect", 3456)
        await fake.enqueue(old_item)
        await wait_until(lambda: not fake.offline, 4, "pre-reconnect fanout")
        replacement = await connect(listen_port, "replacement")
        clients.append(replacement)
        require_event(
            await replacement.commands.send_device_query(),
            EventType.DEVICE_INFO,
            "replacement DEVICE_QUERY",
        )
        empty = await replacement.commands.get_msg(timeout=4)
        require_event(empty, EventType.NO_MORE_MSGS, "replacement historical inbox")
        retained = require_event(
            await modern.commands.get_msg(timeout=4),
            EventType.CONTACT_MSG_RECV,
            "modern retained inbox",
        )
        if retained.payload["text"] != "before-reconnect":
            raise AssertionError("wrong retained pre-reconnect item")

        await fake.enqueue(channel_message("after-reconnect", 4567))
        await wait_until(lambda: not fake.offline, 4, "post-reconnect fanout")
        post = await asyncio.gather(
            modern.commands.get_msg(timeout=4), replacement.commands.get_msg(timeout=4)
        )
        for event in post:
            require_event(event, EventType.CHANNEL_MSG_RECV, "post-reconnect channel")
            if event.payload["text"] != "after-reconnect":
                raise AssertionError("post-reconnect channel body mismatch")

        if not fake.query_targets or set(fake.query_targets) != {13}:
            raise AssertionError(
                f"mux did not normalize every upstream DEVICE_QUERY: {fake.query_targets!r}"
            )
        return {
            "status": "ok",
            "clients": 4,
            "contacts_per_client": 2,
            "unique_time_responses": len(times),
            "fanout_items": 6,
            "duplicate_items_per_client": 2,
            "legacy_backlog_verified": True,
            "upstream_sync_commands": fake.commands[10],
            "upstream_device_query_targets": sorted(set(fake.query_targets)),
        }
    except Exception as exc:
        tail = "\n".join(stderr_tail)
        detail = f"{exc}"
        if tail:
            detail += f"\nmux stderr tail:\n{tail}"
        raise AssertionError(detail) from exc
    finally:
        await asyncio.gather(
            *(client.disconnect() for client in clients), return_exceptions=True
        )
        if process.returncode is None:
            process.send_signal(signal.SIGTERM)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(process.wait(), timeout=4)
        if process.returncode is None:
            process.kill()
            await process.wait()
        await fake.close()
        await stderr_task


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mux-binary", default="out/meshcore-tcp-mux")
    return parser.parse_args()


def main() -> int:
    try:
        result = asyncio.run(run(parse_args()))
    except Exception as exc:
        print(f"check_fake_clients: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
