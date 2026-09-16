#!/usr/bin/env python3
"""Run bounded, non-administrative guest requests against exact contacts.

The check starts a temporary loopback-only mux and resolves every requested
contact by exact case-insensitive name before sending. Each operation is sent
once, without retry. It records native SENT acceptance separately from the
later radio result and runs an unrelated local clock query while the remote
lease is occupied.
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
from typing import Any, Awaitable

from meshcore import EventType, MeshCore
from meshcore.packets import BinaryReqType

from check_clients import connect, require_event
from check_live_reconnect import read_mux_stderr, reserve_port


async def wait_for_terminal(
    tasks: list[asyncio.Task[Any]], timeout: float
) -> Any | None:
    """Return the first actual remote result and cancel alternate waiters."""

    done, pending = await asyncio.wait(
        tasks, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for task in done:
        result = await task
        if result is not None:
            return result
    return None


async def one_remote_operation(
    owner: MeshCore,
    observer: MeshCore,
    name: str,
    send: Awaitable[Any],
    waiters: list[asyncio.Task[Any]],
    timeout: float,
) -> dict[str, Any]:
    """Separate companion acceptance from a correlated asynchronous result."""

    accepted = await send
    if accepted is None or accepted.type == EventType.ERROR:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        return {
            "operation": name,
            "accepted": False,
            "result": "not-sent-or-rejected",
        }
    require_event(accepted, EventType.MSG_SENT, f"{name} acceptance")
    suggested_ms = accepted.payload.get("suggested_timeout")
    if not isinstance(suggested_ms, int) or suggested_ms < 0:
        raise AssertionError(f"{name}: invalid suggested timeout {suggested_ms!r}")
    # The mux protects the lease through the native timeout plus its documented
    # 25%/one-second margin. Never start a competing operation before that
    # interval can have elapsed when a result is missing.
    result_limit = suggested_ms / 1000 * 1.25 + 1
    if result_limit > timeout:
        raise AssertionError(
            f"{name}: required result bound {result_limit:.3f}s exceeds "
            f"--timeout {timeout:.3f}s"
        )
    local_query, terminal = await asyncio.gather(
        observer.commands.get_time(),
        wait_for_terminal(waiters, result_limit),
    )
    require_event(
        local_query, EventType.CURRENT_TIME, f"local query during {name}"
    )
    if terminal is None:
        return {
            "operation": name,
            "accepted": True,
            "suggested_timeout_ms": suggested_ms,
            "result": "timeout",
            "local_query_progressed": True,
        }
    return {
        "operation": name,
        "accepted": True,
        "suggested_timeout_ms": suggested_ms,
        "result": terminal.type.value,
        "result_fields": sorted(terminal.payload.keys())
        if isinstance(terminal.payload, dict)
        else [],
        "correlation_fields": sorted(terminal.attributes.keys()),
        "local_query_progressed": True,
    }


def create_waiter(
    client: MeshCore,
    event_type: EventType,
    filters: dict[str, Any],
    timeout: float,
) -> asyncio.Task[Any]:
    """Register a terminal-result waiter before its command can reach the radio."""

    return asyncio.create_task(
        client.dispatcher.wait_for_event(
            event_type, attribute_filters=filters, timeout=timeout
        )
    )


async def check_target(
    owner: MeshCore,
    observer: MeshCore,
    contact: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    """Run five sequential, single-attempt guest operations for one repeater."""

    prefix = contact["public_key"][:12]
    operations: list[dict[str, Any]] = []

    login_waiters = [
        create_waiter(
            owner, EventType.LOGIN_SUCCESS, {"pubkey_prefix": prefix}, timeout
        ),
        create_waiter(
            owner, EventType.LOGIN_FAILED, {"pubkey_prefix": prefix}, timeout
        ),
    ]
    await asyncio.sleep(0)
    operations.append(
        await one_remote_operation(
            owner,
            observer,
            "login",
            owner.commands.send_login(contact, ""),
            login_waiters,
            timeout,
        )
    )

    status_waiters = [
        create_waiter(
            owner, EventType.STATUS_RESPONSE, {"pubkey_prefix": prefix}, timeout
        )
    ]
    await asyncio.sleep(0)
    operations.append(
        await one_remote_operation(
            owner,
            observer,
            "status",
            owner.commands.send_statusreq(contact),
            status_waiters,
            timeout,
        )
    )

    telemetry_waiters = [
        create_waiter(
            owner,
            EventType.TELEMETRY_RESPONSE,
            {"pubkey_prefix": prefix},
            timeout,
        )
    ]
    await asyncio.sleep(0)
    operations.append(
        await one_remote_operation(
            owner,
            observer,
            "telemetry",
            owner.commands.send_telemetry_req(contact),
            telemetry_waiters,
            timeout,
        )
    )

    path_waiters = [
        create_waiter(
            owner, EventType.PATH_RESPONSE, {"pubkey_pre": prefix}, timeout
        )
    ]
    await asyncio.sleep(0)
    operations.append(
        await one_remote_operation(
            owner,
            observer,
            "path-discovery",
            owner.commands.send_path_discovery(contact),
            path_waiters,
            timeout,
        )
    )

    # BINARY_RESPONSE is routed by the mux's actual SENT tag. Subscribe before
    # sending so a very fast response cannot be lost in client-library setup.
    binary_events: asyncio.Queue[Any] = asyncio.Queue(maxsize=1)

    def capture_binary(event: Any) -> None:
        if binary_events.empty():
            binary_events.put_nowait(event)

    subscription = owner.subscribe(EventType.BINARY_RESPONSE, capture_binary)
    binary_waiter = asyncio.create_task(binary_events.get())
    try:
        operations.append(
            await one_remote_operation(
                owner,
                observer,
                "binary-status",
                owner.commands.send_binary_req(contact, BinaryReqType.STATUS),
                [binary_waiter],
                timeout,
            )
        )
    finally:
        subscription.unsubscribe()

    return {
        # Do not copy deployment-specific names or keys into portable evidence.
        "target": "authorized-repeater",
        "contact_type": contact.get("type"),
        "operations": operations,
        # The high-level anonymous helper may rewrite a flood contact's stored
        # path to zero-hop and back. Persistent contact mutation is outside this
        # goal, so anonymous BASIC/OWNER/REGIONS is deliberately not invoked.
        "anonymous": "skipped-client-helper-may-mutate-contact-path",
        "trace": "pending-safe-path-selection",
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """Resolve exact authorized contacts, run requests, and clean up the mux."""

    binary = Path(args.mux_binary).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise AssertionError(
            f"mux binary is not executable: {binary} (run make first)"
        )

    process: asyncio.subprocess.Process | None = None
    stderr_task: asyncio.Task[None] | None = None
    ready: asyncio.Queue[int] = asyncio.Queue()
    stderr_tail: deque[str] = deque(maxlen=40)
    clients: list[MeshCore] = []
    try:
        listen_port = reserve_port()
        process = await asyncio.create_subprocess_exec(
            str(binary),
            "--upstream-host",
            args.physical_host,
            "--upstream-port",
            str(args.physical_port),
            "--listen-host",
            "127.0.0.1",
            "--listen-multi-client-port",
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
        await asyncio.wait_for(ready.get(), timeout=args.timeout)
        owner, observer = await asyncio.gather(
            connect("127.0.0.1", listen_port, args.timeout, "remote-owner"),
            connect("127.0.0.1", listen_port, args.timeout, "local-observer"),
        )
        clients.extend((owner, observer))

        contacts_event = require_event(
            await owner.commands.get_contacts(timeout=args.timeout),
            EventType.CONTACTS,
            "contacts",
        )
        contacts = list(contacts_event.payload.values())
        selected = []
        for requested in args.target:
            matches = [
                contact
                for contact in contacts
                if contact.get("adv_name", "").casefold() == requested.casefold()
            ]
            if len(matches) != 1:
                raise AssertionError(
                    f"expected exactly one contact matching requested target; "
                    f"found {len(matches)}"
                )
            selected.append(matches[0])

        results = []
        for contact in selected:
            results.append(
                await check_target(owner, observer, contact, args.timeout)
            )
        return {
            "status": "ok",
            "meshcore_py": importlib.metadata.version("meshcore"),
            "targets_resolved": len(selected),
            "single_attempt_operations_per_target": 5,
            "results": results,
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
        if stderr_task:
            await stderr_task


def parse_args() -> argparse.Namespace:
    """Require explicit endpoint and one or more exact authorized target names."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--physical-host", required=True)
    parser.add_argument("--physical-port", required=True, type=int)
    parser.add_argument("--target", required=True, action="append")
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
        print(f"check_remote_requests: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
