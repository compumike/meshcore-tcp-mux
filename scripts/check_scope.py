#!/usr/bin/env python3
"""Verify one temporary-scope send's physical TCP setup/restoration ordering.

The exact authorized repeater is resolved before transmission. The selected
temporary mode is applied only to one mux session and one single-attempt status
request. A metadata-only frame relay proves physical command ordering without
recording the scope key, destination key, response bodies, or identities.
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
import sys
from typing import Any

from meshcore import EventType, MeshCore

from check_clients import connect, require_event
from check_live_reconnect import (
    ExclusiveFrameRelay,
    expect_generation,
    read_mux_stderr,
    reserve_port,
)
from check_remote_requests import create_waiter, one_remote_operation


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """Run one scoped status request and validate the physical command order."""

    binary = Path(args.mux_binary).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise AssertionError(
            f"mux binary is not executable: {binary} (run make first)"
        )

    relay = ExclusiveFrameRelay(args.physical_host, args.physical_port)
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
            env={**os.environ, "LOG_LEVEL": "INFO"},  # Readiness event is required.
        )
        if process.stderr is None:
            raise AssertionError("mux stderr pipe was not created")
        stderr_task = asyncio.create_task(
            read_mux_stderr(process.stderr, ready, stderr_tail)
        )
        await expect_generation(relay.connections, 1)
        await asyncio.wait_for(ready.get(), timeout=args.timeout)

        owner, observer = await asyncio.gather(
            connect("127.0.0.1", listen_port, args.timeout, "scope-owner"),
            connect("127.0.0.1", listen_port, args.timeout, "scope-observer"),
        )
        clients.extend((owner, observer))
        contacts_event = require_event(
            await owner.commands.get_contacts(timeout=args.timeout),
            EventType.CONTACTS,
            "contacts",
        )
        matches = [
            contact
            for contact in contacts_event.payload.values()
            if contact.get("adv_name", "").casefold() == args.target.casefold()
        ]
        if len(matches) != 1:
            raise AssertionError(
                "expected exactly one contact matching --target; "
                f"found {len(matches)}"
            )
        contact = matches[0]
        prefix = contact["public_key"][:12]

        history_start = len(relay.frame_history)
        if args.mode == "test":
            # meshcore_py implements the supported MeshCore derivation:
            # normalize to '#TEST', SHA-256 its UTF-8 bytes, and use 16 bytes.
            scope_result = await owner.commands.set_flood_scope("TEST")
            setup_payload_bytes = 18
        elif args.mode == "unscoped":
            scope_result = await owner.commands.force_unscoped()
            setup_payload_bytes = 2
        else:
            raise AssertionError(f"unknown scope mode {args.mode}")
        require_event(scope_result, EventType.OK, "virtual scope selection")

        # SET_FLOOD_SCOPE_KEY is virtual until a scope-sensitive command is
        # dispatched, so selecting it must not have changed physical state yet.
        before_send = list(relay.frame_history)[history_start:]
        if any(
            frame.direction == "command" and frame.opcode == 0x36
            for frame in before_send
        ):
            raise AssertionError("virtual scope selection leaked upstream early")

        waiter = create_waiter(
            owner,
            EventType.STATUS_RESPONSE,
            {"pubkey_prefix": prefix},
            args.timeout,
        )
        await asyncio.sleep(0)
        operation = await one_remote_operation(
            owner,
            observer,
            f"{args.mode}-scope-status",
            owner.commands.send_statusreq(contact),
            [waiter],
            args.timeout,
        )
        # Reset only this session's virtual preference. The mux has already
        # required a real physical default restoration after the status SENT.
        require_event(
            await owner.commands.reset_flood_scope(),
            EventType.OK,
            "virtual scope reset",
        )

        commands = [
            frame
            for frame in list(relay.frame_history)[history_start:]
            if frame.direction == "command"
            and frame.opcode in (0x36, 0x1B, 0x05)
        ]
        opcodes = [frame.opcode for frame in commands]
        if opcodes != [0x36, 0x1B, 0x36, 0x05]:
            raise AssertionError(
                "unexpected scoped command sequence "
                + ",".join(f"0x{opcode:02x}" for opcode in opcodes)
            )
        # TCP envelope adds three bytes. Setup is either explicit key (18-byte
        # payload) or unscoped (2); restoration is always [0x36, 0x00] (2).
        expected_frame_bytes = [
            setup_payload_bytes + 3,
            33 + 3,
            2 + 3,
            1 + 3,
        ]
        actual_frame_bytes = [frame.frame_bytes for frame in commands]
        if actual_frame_bytes != expected_frame_bytes:
            raise AssertionError(
                f"unexpected scoped frame lengths {actual_frame_bytes!r}"
            )

        return {
            "status": "ok",
            "meshcore_py": importlib.metadata.version("meshcore"),
            "mode": args.mode,
            "target_resolved": True,
            "operation": operation,
            "physical_command_opcodes": [
                "set-temporary-scope",
                "send-status-request-once",
                "restore-default-scope",
                "other-client-clock-query",
            ],
            "physical_frame_bytes": actual_frame_bytes,
            "virtual_selection_was_local": True,
            "tcp_order_verified": True,
            "packet_scope_observer": "unavailable",
        }
    except Exception as exc:
        detail = str(exc)
        if stderr_tail:
            detail += "\nmux stderr tail:\n" + "\n".join(stderr_tail)
        raise AssertionError(detail) from exc
    finally:
        await asyncio.gather(
            *(client.disconnect() for client in clients), return_exceptions=True
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
    """Require an explicit physical endpoint, contact, and safe scope mode."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--physical-host", required=True)
    parser.add_argument("--physical-port", required=True, type=int)
    parser.add_argument("--target", required=True)
    parser.add_argument("--mode", required=True, choices=("test", "unscoped"))
    parser.add_argument("--mux-binary", default="out/meshcore-tcp-mux")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()
    if not 1 <= args.physical_port <= 65535:
        parser.error("--physical-port must be between 1 and 65535")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def main() -> int:
    """Return a shell-friendly status and privacy-safe JSON evidence."""

    try:
        result = asyncio.run(run(parse_args()))
    except Exception as exc:
        print(f"check_scope: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
