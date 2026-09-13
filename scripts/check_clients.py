#!/usr/bin/env python3
"""Read-only interoperability check using two independent meshcore_py clients."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import sys
from typing import Any

from meshcore import EventType, MeshCore


def require_event(event: Any, expected: EventType, operation: str) -> Any:
    if event is None:
        raise AssertionError(f"{operation}: no event returned")
    if event.type == EventType.ERROR:
        raise AssertionError(f"{operation}: device/client error {event.payload!r}")
    if event.type != expected:
        raise AssertionError(
            f"{operation}: expected {expected.value}, got {event.type.value}"
        )
    return event


async def connect(host: str, port: int, timeout: float, label: str) -> MeshCore:
    client = await MeshCore.create_tcp(
        host,
        port,
        only_error=True,
        default_timeout=timeout,
        auto_reconnect=False,
    )
    if client is None:
        raise AssertionError(f"{label}: APP_START failed")
    if len(client.self_info.get("public_key", "")) != 64:
        raise AssertionError(f"{label}: malformed SELF_INFO identity")
    return client


async def query_read_only(client: MeshCore, label: str, iteration: int) -> dict[str, Any]:
    # Calls are intentionally sequential within one connection. The two callers
    # run concurrently, exercising mux ownership without relying on event-only
    # correlation inside a single meshcore_py session.
    device = require_event(
        await client.commands.send_device_query(),
        EventType.DEVICE_INFO,
        f"{label} device query {iteration}",
    )
    if device.payload.get("fw ver") != 13:
        raise AssertionError(
            f"{label}: expected native protocol 13, got {device.payload.get('fw ver')!r}"
        )

    current_time = require_event(
        await client.commands.get_time(),
        EventType.CURRENT_TIME,
        f"{label} time query {iteration}",
    )
    timestamp = current_time.payload.get("time")
    if not isinstance(timestamp, int) or timestamp <= 0:
        raise AssertionError(f"{label}: invalid device time {timestamp!r}")

    contacts = require_event(
        await client.commands.get_contacts(timeout=10),
        EventType.CONTACTS,
        f"{label} contacts query {iteration}",
    )
    if not isinstance(contacts.payload, dict):
        raise AssertionError(f"{label}: malformed contacts result")

    return {
        "protocol": device.payload["fw ver"],
        "firmware": device.payload.get("ver", ""),
        "build": device.payload.get("fw_build", ""),
        "contacts": len(contacts.payload),
        "time": timestamp,
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    clients: list[MeshCore] = []
    try:
        first, second = await asyncio.gather(
            connect(args.host, args.port, args.timeout, "client-a"),
            connect(args.host, args.port, args.timeout, "client-b"),
        )
        clients.extend((first, second))
        if first.self_info["public_key"] != second.self_info["public_key"]:
            raise AssertionError("clients observed different companion identities")

        rounds = []
        for iteration in range(args.iterations):
            pair = await asyncio.gather(
                query_read_only(first, "client-a", iteration),
                query_read_only(second, "client-b", iteration),
            )
            if pair[0]["contacts"] != pair[1]["contacts"]:
                raise AssertionError(
                    f"round {iteration}: contact counts differ: "
                    f"{pair[0]['contacts']} != {pair[1]['contacts']}"
                )
            rounds.append(pair)

        # Exercise a new downstream session explicitly rather than relying on
        # meshcore_py's optional transport auto-reconnect.
        await first.disconnect()
        clients.remove(first)
        replacement = await connect(
            args.host, args.port, args.timeout, "client-a-reconnected"
        )
        clients.append(replacement)
        if replacement.self_info["public_key"] != second.self_info["public_key"]:
            raise AssertionError("reconnected client observed a different identity")
        reconnect = await query_read_only(
            replacement, "client-a-reconnected", args.iterations
        )
        if reconnect["contacts"] != rounds[-1][1]["contacts"]:
            raise AssertionError("contact count changed across read-only reconnect check")

        return {
            "status": "ok",
            "meshcore_py": importlib.metadata.version("meshcore"),
            "meshcore_cli": importlib.metadata.version("meshcore-cli"),
            "host": args.host,
            "port": args.port,
            "iterations": args.iterations,
            "protocol": reconnect["protocol"],
            "firmware": reconnect["firmware"],
            "build": reconnect["build"],
            "contact_count": reconnect["contacts"],
            "sessions_checked": 3,
            "queries_completed": args.iterations * 2 + 1,
        }
    finally:
        await asyncio.gather(
            *(client.disconnect() for client in clients), return_exceptions=True
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--iterations", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.iterations < 1:
        parser.error("--iterations must be at least one")
    return args


def main() -> int:
    try:
        result = asyncio.run(run(parse_args()))
    except Exception as exc:
        print(f"check_clients: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
