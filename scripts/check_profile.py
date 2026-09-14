#!/usr/bin/env python3
"""Read only a companion profile and print privacy-safe capability metadata."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import sys
from typing import Any

from meshcore import EventType, MeshCore

from check_clients import require_event


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """Query version, clock, battery, and self telemetry without inbox access."""

    client = await MeshCore.create_tcp(
        args.host,
        args.port,
        only_error=True,
        default_timeout=args.timeout,
        auto_reconnect=False,
    )
    if client is None:
        raise AssertionError("APP_START failed")
    try:
        device = require_event(
            await client.commands.send_device_query(),
            EventType.DEVICE_INFO,
            "device query",
        )
        current_time = require_event(
            await client.commands.get_time(), EventType.CURRENT_TIME, "clock"
        )
        battery = require_event(
            await client.commands.get_bat(), EventType.BATTERY, "battery"
        )
        telemetry = require_event(
            await client.commands.get_self_telemetry(),
            EventType.TELEMETRY_RESPONSE,
            "self telemetry",
        )
        return {
            "status": "ok",
            "profile_label": args.label,
            "meshcore_py": importlib.metadata.version("meshcore"),
            "model": device.payload.get("model", ""),
            "firmware": device.payload.get("ver", ""),
            "build": device.payload.get("fw_build", ""),
            "protocol": device.payload.get("fw ver"),
            "clock_is_positive": current_time.payload.get("time", 0) > 0,
            "battery_fields": sorted(battery.payload.keys()),
            "self_telemetry_fields": sorted(telemetry.payload.keys()),
            # Identity equality can be checked without printing the public key.
            "identity_bytes": len(bytes.fromhex(client.self_info["public_key"])),
        }
    finally:
        await client.disconnect()


def parse_args() -> argparse.Namespace:
    """Parse an explicit endpoint and a non-identifying evidence label."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--label", required=True)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def main() -> int:
    """Return a shell-friendly status and one privacy-safe JSON line."""

    try:
        result = asyncio.run(run(parse_args()))
    except Exception as exc:
        print(f"check_profile: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
