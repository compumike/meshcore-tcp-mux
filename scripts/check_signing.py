#!/usr/bin/env python3
"""Verify mux signing ownership and an actual companion Ed25519 signature.

The check starts a temporary loopback-only mux, signs fixed synthetic bytes in
multiple chunks on the selected physical companion, and verifies the returned
signature independently with OpenSSL. It never exports key material, transmits
radio traffic, or changes persistent configuration.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from collections import deque
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from typing import Any

from meshcore import EventType, MeshCore

from check_clients import connect, require_event
from check_live_reconnect import read_mux_stderr, reserve_port


SYNTHETIC_DOCUMENT = (
    b"meshcore-tcp-mux signing check\x00"
    b"fixed non-secret payload\x3c\x3e"
)


def verify_ed25519(public_key: bytes, document: bytes, signature: bytes) -> None:
    """Verify raw Ed25519 material through OpenSSL's independent implementation."""

    if len(public_key) != 32:
        raise AssertionError(f"expected a 32-byte public key, got {len(public_key)}")
    if len(signature) != 64:
        raise AssertionError(f"expected a 64-byte signature, got {len(signature)}")

    # RFC 8410 SubjectPublicKeyInfo prefix for the Ed25519 object identifier,
    # followed by a BIT STRING containing the raw 32-byte public key.
    public_key_der = bytes.fromhex("302a300506032b6570032100") + public_key
    with tempfile.TemporaryDirectory(prefix="meshcore-signing-") as directory:
        work = Path(directory)
        public_path = work / "public.der"
        document_path = work / "document.bin"
        signature_path = work / "signature.bin"
        public_path.write_bytes(public_key_der)
        document_path.write_bytes(document)
        signature_path.write_bytes(signature)
        completed = subprocess.run(
            [
                "openssl",
                "pkeyutl",
                "-verify",
                "-pubin",
                "-inkey",
                str(public_path),
                "-keyform",
                "DER",
                "-rawin",
                "-in",
                str(document_path),
                "-sigfile",
                str(signature_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )
    if completed.returncode != 0:
        raise AssertionError(
            "OpenSSL rejected the companion signature: "
            + completed.stderr.strip()
        )


async def sign_data(client: MeshCore, chunk: bytes) -> None:
    """Send one SIGN_DATA and require a real native OK rather than masking timeout."""

    # SIGN_DATA (0x22) carries the literal document chunk after its opcode.
    event = await client.commands.send(
        b"\x22" + chunk, [EventType.OK, EventType.ERROR], timeout=5
    )
    require_event(event, EventType.OK, "SIGN_DATA")


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """Run one bounded two-client signing conversation and clean up all sockets."""

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

        owner, competitor = await asyncio.gather(
            connect("127.0.0.1", listen_port, args.timeout, "signing-owner"),
            connect("127.0.0.1", listen_port, args.timeout, "signing-competitor"),
        )
        clients.extend((owner, competitor))
        if owner.self_info["public_key"] != competitor.self_info["public_key"]:
            raise AssertionError("clients observed different companion identities")

        started = require_event(
            await owner.commands.sign_start(), EventType.SIGN_START, "SIGN_START"
        )
        maximum = started.payload.get("max_length")
        if not isinstance(maximum, int) or maximum < len(SYNTHETIC_DOCUMENT):
            raise AssertionError(
                f"signing limit {maximum!r} is smaller than the test document"
            )

        denied = await competitor.commands.sign_start()
        if denied.type != EventType.ERROR:
            raise AssertionError("a competing SIGN_START was not rejected")

        # A local query remains safe while the physical signing buffer belongs
        # to another client. It must not release or replace that signing lease.
        require_event(
            await competitor.commands.get_time(),
            EventType.CURRENT_TIME,
            "clock query during signing",
        )

        chunks = [
            SYNTHETIC_DOCUMENT[:11],
            SYNTHETIC_DOCUMENT[11:37],
            SYNTHETIC_DOCUMENT[37:],
        ]
        for chunk in chunks:
            await sign_data(owner, chunk)
        finished = require_event(
            await owner.commands.sign_finish(
                timeout=args.timeout, data_size=len(SYNTHETIC_DOCUMENT)
            ),
            EventType.SIGNATURE,
            "SIGN_FINISH",
        )
        signature = finished.payload.get("signature")
        if not isinstance(signature, bytes):
            raise AssertionError("SIGN_FINISH did not return byte signature material")
        public_key = bytes.fromhex(owner.self_info["public_key"])
        verify_ed25519(public_key, SYNTHETIC_DOCUMENT, signature)

        return {
            "status": "ok",
            "meshcore_py": importlib.metadata.version("meshcore"),
            "document_sha256": hashlib.sha256(SYNTHETIC_DOCUMENT).hexdigest(),
            "document_bytes": len(SYNTHETIC_DOCUMENT),
            "chunks": len(chunks),
            "advertised_max_bytes": maximum,
            "competitor_rejected": True,
            "local_query_during_lease": True,
            "signature_bytes": len(signature),
            "openssl_verified": True,
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
    """Parse an explicit physical endpoint and finite operation timeout."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--physical-host", required=True)
    parser.add_argument("--physical-port", required=True, type=int)
    parser.add_argument("--mux-binary", default="out/meshcore-tcp-mux")
    parser.add_argument("--timeout", type=float, default=15.0)
    args = parser.parse_args()
    if not 1 <= args.physical_port <= 65535:
        parser.error("--physical-port must be between 1 and 65535")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def main() -> int:
    """Return a shell-friendly status and one privacy-safe JSON evidence line."""

    try:
        result = asyncio.run(run(parse_args()))
    except Exception as exc:
        print(f"check_signing: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
