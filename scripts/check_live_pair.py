#!/usr/bin/env python3
"""Exercise two explicitly selected physical companions through temporary muxes.

Read-only by default. --execute sends bounded DMs in both directions and one
message in each direction on the explicitly named channel, using the existing
live dedicated-client harness, plus four raw inbox dialect sends. No node is rebooted or reconfigured. Addresses,
contact names, channel keys, and live message bodies are not retained in results.
Other command producers must be stopped while this harness owns the companions.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
from collections import deque
import json
import os
from pathlib import Path
import signal
import secrets
import time
from types import SimpleNamespace
from typing import Any

from meshcore import EventType

from check_clients import connect, require_event, run as check_clients
from check_live_dedicated import AckCollector, get_contacts, resolve_channel, run as check_dedicated
from check_live_reconnect import (
    ExclusiveFrameRelay, expect_generation, read_mux_stderr, reserve_port,
    run as check_reconnect,
)
from check_profile import run as check_profile


class PhysicalMux:
    """Own a temporary mux, its frame relay, and loopback-only listener ports."""

    def __init__(self, host: str, port: int, binary: str) -> None:
        self.relay = ExclusiveFrameRelay(host, port)
        self.binary = str(Path(binary).resolve())
        self.multi_port = reserve_port()
        self.dedicated_ports = [reserve_port(), reserve_port()]
        self.process: asyncio.subprocess.Process | None = None
        self.stderr_task: asyncio.Task[None] | None = None
        self.ready: asyncio.Queue[int] = asyncio.Queue()
        self.stderr_tail: deque[str] = deque(maxlen=40)

    async def start(self) -> None:
        """Fence the genuine upstream before any client is admitted."""
        await self.relay.start()
        arguments = [
            self.binary, "--upstream-host", "127.0.0.1", "--upstream-port", str(self.relay.port),
            "--listen-host", "127.0.0.1", "--listen-multi-client-port", str(self.multi_port),
        ]
        for port in self.dedicated_ports:
            arguments.extend(("--listen-dedicated-client-port", str(port)))
        self.process = await asyncio.create_subprocess_exec(
            *arguments, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "LOG_LEVEL": "INFO"},
        )
        assert self.process.stderr
        self.stderr_task = asyncio.create_task(
            read_mux_stderr(self.process.stderr, self.ready, self.stderr_tail)
        )
        await expect_generation(self.relay.connections, 1)
        await expect_generation(self.ready, 1)

    async def stop(self) -> None:
        """Join the mux and relay so later tests cannot compete for physical TCP."""
        if self.process and self.process.returncode is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()
        await self.relay.close()
        if self.stderr_task:
            await self.stderr_task


async def stress(mux: PhysicalMux, level: int, rounds: int) -> dict[str, Any]:
    """Overlap four clients' differing reply grammars without concurrent calls per socket."""
    clients = []
    try:
        for index in range(4):
            clients.append(await connect("127.0.0.1", mux.multi_port, 10, f"stress-{index}"))

        async def worker(index: int, client: Any) -> int:
            completed = 0
            for iteration in range(rounds):
                # DEVICE_QUERY (22) stores the client target; it never changes the mux's dialect.
                target = (0, 2, 3, 14, 255)[(index + iteration) % 5]
                info = require_event(
                    await client.commands.send(bytes([22, target]), [EventType.DEVICE_INFO, EventType.ERROR]),
                    EventType.DEVICE_INFO, "client dialect query",
                )
                if info.payload["fw ver"] != level:
                    raise AssertionError("client target changed advertised firmware")
                completed += 1
                calls = (
                    (client.commands.get_time, EventType.CURRENT_TIME),
                    (client.commands.get_bat, EventType.BATTERY),
                    (client.commands.get_stats_core, EventType.STATS_CORE),
                    (client.commands.get_stats_radio, EventType.STATS_RADIO),
                    (client.commands.get_stats_packets, EventType.STATS_PACKETS),
                    (client.commands.get_self_telemetry, EventType.TELEMETRY_RESPONSE),
                )
                operation, kind = calls[(iteration + index) % len(calls)]
                require_event(await operation(), kind, f"client-{index} read-only {iteration}")
                completed += 1
                # Distinct replies expose cross-client theft: ver differs from an unknown command.
                cli = await client.commands.run_cli_command("ver" if index % 2 == 0 else "mux-hil-unknown-command")
                if level == 13:
                    require_event_type = EventType.ERROR  # v13 ERR_UNSUPPORTED_CMD is a normal terminal reply.
                    if cli is None or cli.type != require_event_type or cli.payload.get("error_code") != 1:
                        raise AssertionError("v13 CLI command did not terminate with ERR")
                else:
                    require_event(cli, EventType.CLI_REPLY, "local CLI")
                    text = cli.payload["text"]
                    if (index % 2 == 1) != (text == "Unknown command"):
                        raise AssertionError("CLI response reached the wrong client")
                completed += 1
            return completed

        counts = await asyncio.gather(*(worker(i, c) for i, c in enumerate(clients)))
        return {"status": "ok", "clients": 4, "rounds_per_client": rounds, "completed_commands": sum(counts)}
    finally:
        await asyncio.gather(*(c.disconnect() for c in clients), return_exceptions=True)


async def isolation(mux: PhysicalMux) -> dict[str, Any]:
    """Reject invalid clients locally while a healthy session keeps its upstream epoch."""
    client = await connect("127.0.0.1", mux.multi_port, 10, "isolation")
    try:
        # Reserved command, truncated DEVICE_QUERY, and empty RUN_CLI_COMMAND.
        # ERR_UNSUPPORTED_CMD=1 and ERR_ILLEGAL_ARG=6 are local terminal errors.
        for payload, reason in ((bytes([0xfe]), 1), (bytes([22]), 6), (bytes([66]), 6)):
            event = await client.commands.send(payload, [EventType.ERROR])
            if event.type != EventType.ERROR or event.payload.get("error_code") != reason:
                raise AssertionError("invalid command received the wrong local error")
            require_event(await client.commands.get_time(), EventType.CURRENT_TIME, "clock after local error")

        # TCP envelope errors: wrong direction marker, zero payload length,
        # and an announced 177-byte payload beyond the 176-byte wire profile.
        for frame in (b">\x01\x00\x05", b"<\x00\x00", b"<\xb1\x00"):
            reader, writer = await asyncio.open_connection("127.0.0.1", mux.multi_port)
            try:
                writer.write(frame)
                await writer.drain()
                async def closed() -> None:
                    while await reader.read(1024):
                        pass
                await asyncio.wait_for(closed(), 3)
            finally:
                writer.close()
                with contextlib.suppress(ConnectionError):
                    await writer.wait_closed()
            require_event(await client.commands.get_time(), EventType.CURRENT_TIME, "clock after malformed client")
        for index in range(10):
            replacement = await connect("127.0.0.1", mux.multi_port, 10, f"churn-{index}")
            try:
                require_event(await replacement.commands.get_time(), EventType.CURRENT_TIME, "replacement clock")
                require_event(await client.commands.get_time(), EventType.CURRENT_TIME, "healthy clock during churn")
            finally:
                await replacement.disconnect()
        if not mux.relay.active or mux.relay.active.generation != 1:
            raise AssertionError("client error or churn unexpectedly replaced the upstream epoch")
        return {"status": "ok", "local_rejections": 3, "malformed_clients_closed": 3, "replacements": 10, "upstream_epoch_unchanged": True}
    finally:
        await client.disconnect()


class InboxWireClient:
    """Own one raw downstream socket so inbox response dialects are checked before SDK decoding."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, port: int, target: int) -> "InboxWireClient":
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        client = cls(reader, writer)
        try:
            # APP_START (1): seven reserved bytes plus a synthetic application name.
            await client.command(bytes([1]) + bytes(7) + b"mux-dialect-test")
            info = await client.reply()
            if info[0] != 5:  # SELF_INFO identifies the shared companion.
                raise AssertionError("raw APP_START did not return SELF_INFO")
            # DEVICE_QUERY (22): retain this client's target independently of exposed firmware.
            await client.command(bytes([22, target]))
            info = await client.reply()
            if info[0] != 13 or len(info) != 82:  # DEVICE_INFO: known capability prefix.
                raise AssertionError("raw query did not return the known DEVICE_INFO shape")
            return client
        except BaseException:
            await client.close()
            raise

    async def command(self, payload: bytes) -> None:
        """Encode the '<' marker and two-byte LE payload length, excluding the envelope."""
        self.writer.write(b"<" + len(payload).to_bytes(2, "little") + payload)
        await self.writer.drain()

    async def reply(self) -> bytes:
        """Skip asynchronous pushes; retain exactly one owned ordinary reply."""
        while True:
            header = await asyncio.wait_for(self.reader.readexactly(3), 15)
            size = int.from_bytes(header[1:], "little")
            if header[0] != ord(">") or not 1 <= size <= 176:
                raise AssertionError("invalid raw downstream envelope")
            payload = await asyncio.wait_for(self.reader.readexactly(size), 15)
            if payload[0] < 0x80:  # High codes are asynchronous broadcasts, not sync results.
                return payload

    async def marker(self, body: bytes, channel: bool) -> bytes:
        """Pop until one exact synthetic marker is received, with bounded background traffic."""
        deadline = asyncio.get_running_loop().time() + 30
        unmatched = 0
        while asyncio.get_running_loop().time() < deadline:
            await self.command(bytes([10]))  # SYNC_NEXT_MESSAGE requests this client's queue.
            payload = await self.reply()
            if payload[0] == 10:  # NO_MORE_MESSAGES is progress, not receipt.
                await asyncio.sleep(0.1)
                continue
            if payload[0] == 1:  # ERR: synchronization was rejected, not a received message.
                raise AssertionError("raw inbox synchronization was rejected")
            # CONTACT_MESSAGE (7) / CONTACT_MESSAGE_V3 (16) have body offsets 13/16;
            # CHANNEL_MESSAGE (8) / CHANNEL_MESSAGE_V3 (17) have body offsets 8/11.
            offsets = {7: 13, 16: 16, 8: 8, 17: 11}
            expected = (8, 17) if channel else (7, 16)
            if payload[0] in expected:
                text = payload[offsets[payload[0]]:]
                matches = text.endswith(b": " + body) if channel else text == body
                if matches:
                    return payload
            unmatched += 1
            if unmatched > 64:
                raise AssertionError("raw inbox exceeded the background-message bound")
        raise AssertionError("raw inbox marker was not received")

    async def close(self) -> None:
        self.writer.close()
        with contextlib.suppress(ConnectionError):
            await self.writer.wait_closed()


async def dialect_radio(muxes: list[PhysicalMux], channel_name: str) -> dict[str, Any]:
    """Use four real radio sends to compare legacy and V3 inbox copies byte-for-byte."""
    senders = []
    receivers: list[list[InboxWireClient]] = []
    collector = AckCollector()
    try:
        for index, mux in enumerate(muxes):
            sender = await connect("127.0.0.1", mux.multi_port, 10, f"dialect-sender-{index}")
            senders.append(sender)
            collector.attach(str(index), sender)
            pair = []
            receivers.append(pair)
            for target in (2, 3):
                pair.append(await InboxWireClient.connect(mux.multi_port, target))
        destinations = []
        channels = []
        for index, sender in enumerate(senders):
            contacts = await get_contacts(sender, "dialect-sender")
            matches = [c for c in contacts.values() if c.get("public_key") == senders[1-index].self_info["public_key"]]
            if len(matches) != 1:
                raise AssertionError("raw dialect test lacks a reciprocal contact")
            destinations.append(matches[0])
            channel, _ = await resolve_channel(sender, channel_name, "dialect-sender")
            channels.append(channel)
        run_id = secrets.token_hex(4)
        comparisons = 0
        for channel in (False, True):
            for index, sender in enumerate(senders):
                body = f"mux-dialect {run_id} {index} {channel}"
                if channel:
                    sent = await sender.commands.send_chan_msg(channels[index], body, timestamp=int(time.time()))
                    require_event(sent, EventType.OK, "dialect channel acceptance")
                else:
                    sent = await sender.commands.send_msg(destinations[index], body, timestamp=int(time.time()), attempt=0)
                    require_event(sent, EventType.MSG_SENT, "dialect DM acceptance")
                legacy, modern = await asyncio.gather(*(c.marker(body.encode(), channel) for c in receivers[1-index]))
                # V3 adds SNR plus two reserved bytes at offsets 1..3. Removing
                # them and changing the opcode must exactly reproduce the legacy copy.
                legacy_code, modern_code = (8, 17) if channel else (7, 16)
                if legacy[0] != legacy_code or modern[0] != modern_code:
                    raise AssertionError("one client changed another client's inbox dialect")
                if legacy != bytes([legacy_code]) + modern[4:]:
                    raise AssertionError("legacy downgrade changed the radio message header or body")
                if not channel:
                    await collector.require_token(sent.payload["expected_ack"].hex(), [str(index)], max(5, sent.payload["suggested_timeout"] / 1000 * 1.25 + 1))
                comparisons += 1
        return {"status": "ok", "radio_sends": 4, "byte_exact_comparisons": comparisons, "client_targets": [2, 3]}
    finally:
        await asyncio.gather(*(c.close() for pair in receivers for c in pair), return_exceptions=True)
        await asyncio.gather(*(c.disconnect() for c in senders), return_exceptions=True)


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """Test both physical profiles, two radio directions, then isolated fault recovery."""
    endpoints = [(args.a_host, args.a_port), (args.b_host, args.b_port)]
    muxes = [PhysicalMux(host, port, args.mux_binary) for host, port in endpoints]
    results: dict[str, Any] = {"execute": args.execute, "nodes": [], "radio": [], "reconnect": []}
    try:
        for mux in muxes:
            await mux.start()
        for index, mux in enumerate(muxes):
            profile = await check_profile(SimpleNamespace(host="127.0.0.1", port=mux.multi_port, label=f"node-{index}", timeout=10))
            concurrency = await check_clients(SimpleNamespace(host="127.0.0.1", port=mux.multi_port, timeout=10, iterations=5))
            results["nodes"].append({"profile": profile, "concurrency": {k:v for k,v in concurrency.items() if k not in ("host", "port")}, "stress": await stress(mux, profile["protocol"], args.stress_rounds), "isolation": await isolation(mux)})
            print(json.dumps({"event": "read_only_complete", "node": index, "protocol": profile["protocol"], "commands": results["nodes"][-1]["stress"]["completed_commands"]}), flush=True)

        # Match the reciprocal contact using full identities obtained from the live endpoints.
        clients = []
        try:
            for index, mux in enumerate(muxes):
                clients.append(await connect("127.0.0.1", mux.multi_port, 10, f"preflight-{index}"))
            names = []
            secrets = []
            for index, client in enumerate(clients):
                contacts = await get_contacts(client, f"node-{index}")
                matches = [c for c in contacts.values() if c.get("public_key") == clients[1-index].self_info["public_key"]]
                if len(matches) != 1:
                    raise AssertionError("expected exactly one reciprocal live contact")
                names.append(matches[0]["adv_name"])
                _, secret = await resolve_channel(client, args.channel, f"node-{index}")
                secrets.append(secret)
            if secrets[0] != secrets[1]:
                raise AssertionError("test channel secrets differ")
        finally:
            await asyncio.gather(*(c.disconnect() for c in clients), return_exceptions=True)

        if args.execute:
            for index, mux in enumerate(muxes):
                peer = muxes[1-index]
                # Three live sending sessions, inbound fanout, two retained DMs,
                # a dedicated takeover, and one channel message: exactly eight sends.
                try:
                    result = await check_dedicated(SimpleNamespace(
                        mux_host="127.0.0.1", multi_port=mux.multi_port, dedicated_ports=mux.dedicated_ports,
                        peer_host="127.0.0.1", peer_port=peer.multi_port,
                        mux_contact=names[1-index], peer_contact=names[index], channel=args.channel,
                        query_rounds=5, timeout=10, receive_timeout=60,
                        max_unmatched=64, max_radio_sends=8, execute=True,
                    ))
                except Exception as exc:
                    result = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
                    print(json.dumps({"event": "radio_failed", "node": index, **result}), flush=True)
                results["radio"].append({"mux_node": index, **result})
            results["dialect_radio"] = await dialect_radio(muxes, args.channel)
    finally:
        # SEND_CONFIRMED (0x82) is a genuine upstream ACK; count it separately
        # from the received DM body and from the companion's SENT acceptance.
        results["upstream_ack_frames"] = [
            mux.relay.count_frames(1, "response", 0x82) for mux in muxes
        ]
        for mux in muxes:
            await mux.stop()

    if args.reconnect:
        for index, (host, port) in enumerate(endpoints):
            for scenario in ("completed-drop", "single-timeout", "contacts-timeout"):
                result = await check_reconnect(SimpleNamespace(
                    physical_host=host, physical_port=port, mux_binary=args.mux_binary,
                    timeout=15, response_timeout=2, scenario=scenario,
                ))
                results["reconnect"].append({"node": index, **result})
                print(json.dumps({"event": "reconnect_complete", "node": index, "scenario": scenario}), flush=True)
    results["status"] = "ok" if all(r["status"] == "ok" for r in results["radio"]) else "failed"
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Require explicit physical destinations and an explicit radio opt-in."""
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("a", "b"):
        parser.add_argument(f"--{label}-host", required=True)
        parser.add_argument(f"--{label}-port", required=True, type=int)
    parser.add_argument("--mux-binary", default="out/meshcore-tcp-mux")
    parser.add_argument("--channel", required=True)
    parser.add_argument("--stress-rounds", type=int, default=15)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--reconnect", action="store_true")
    parser.add_argument("--results", type=Path)
    args = parser.parse_args(argv)
    if any(not 1 <= port <= 65535 for port in (args.a_port, args.b_port)):
        parser.error("physical ports must be between 1 and 65535")
    if not 1 <= args.stress_rounds <= 100:
        parser.error("stress rounds must be between 1 and 100")
    if (args.a_host, args.a_port) == (args.b_host, args.b_port):
        parser.error("physical companions must have distinct endpoints")
    return args


def main() -> int:
    """Return a bounded, metadata-only result and conventional process status."""
    args = parse_args()
    try:
        results = asyncio.run(run(args))
    except Exception as exc:
        print(f"check_live_pair: FAIL: {exc}", flush=True)
        return 1
    if args.results:
        args.results.write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, sort_keys=True))
    return 0 if results["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
