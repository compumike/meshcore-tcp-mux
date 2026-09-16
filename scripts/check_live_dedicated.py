#!/usr/bin/env python3
"""Bounded real-radio verification for multi-client and dedicated listeners.

The default run is read-only: it verifies identities, exact reciprocal contacts,
and concurrent commands. ``--execute`` additionally sends uniquely marked DMs
without retries, exercises detached backfill and dedicated connection takeover,
and optionally sends one channel message. Message bodies are never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import secrets
import sys
import time
from dataclasses import dataclass
from typing import Any

from meshcore import EventType, MeshCore

from check_clients import connect, require_event


INBOX_TYPES = (EventType.CONTACT_MSG_RECV, EventType.CHANNEL_MSG_RECV)


def emit(event: str, **details: Any) -> None:
    """Write one promptly flushed, metadata-only progress record."""
    print(json.dumps({"event": event, **details}, sort_keys=True), flush=True)


def marker_digest(marker: str) -> str:
    """Identify a synthetic marker without copying its body into reports."""
    return hashlib.sha256(marker.encode()).hexdigest()


def resolve_contact(
    contacts: dict[str, dict[str, Any]],
    expected_name: str,
    expected_public_key: str,
    endpoint: str,
) -> dict[str, Any]:
    """Fence a destination by both exact advertised name and full live identity."""
    matches = [
        contact
        for contact in contacts.values()
        if contact.get("adv_name", "").casefold() == expected_name.casefold()
        and contact.get("public_key", "").casefold()
        == expected_public_key.casefold()
    ]
    if len(matches) != 1:
        raise AssertionError(
            f"{endpoint}: expected one contact named {expected_name!r} with "
            f"identity {expected_public_key[:12]}, found {len(matches)}"
        )
    return matches[0]


@dataclass
class SendBudget:
    """Prevent a failed phase from expanding into an unbounded radio test."""

    limit: int
    used: int = 0

    def consume(self, description: str) -> None:
        """Reserve exactly one transmission before issuing its command."""
        if self.used >= self.limit:
            raise AssertionError(
                f"radio send budget exhausted before {description}: "
                f"limit={self.limit}"
            )
        self.used += 1


class AckCollector:
    """Collect broadcast delivery confirmations independently per mux session."""

    def __init__(self) -> None:
        self.queues: dict[str, asyncio.Queue[dict[str, Any]]] = {}

    def attach(self, label: str, client: MeshCore) -> None:
        """Subscribe before sending so a fast ACK cannot race the waiter."""
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.queues[label] = queue

        async def collect(event: Any) -> None:
            await queue.put(event.payload)

        client.subscribe(EventType.ACK, collect)

    async def require_token(
        self, token: str, labels: list[str], timeout: float
    ) -> None:
        """Require the same genuine ACK push on every selected mux connection."""

        async def matching(label: str) -> dict[str, Any]:
            queue = self.queues[label]
            while True:
                payload = await queue.get()
                if payload.get("code") == token:
                    return payload

        receipts = await asyncio.wait_for(
            asyncio.gather(*(matching(label) for label in labels)), timeout
        )
        if any(receipt != receipts[0] for receipt in receipts[1:]):
            raise AssertionError("mux sessions observed different ACK payloads")


async def get_contacts(client: MeshCore, label: str) -> dict[str, dict[str, Any]]:
    """Fetch one complete contact stream and reject partial/error results."""
    event = require_event(
        await client.commands.get_contacts(timeout=15),
        EventType.CONTACTS,
        f"{label} contacts",
    )
    if not isinstance(event.payload, dict):
        raise AssertionError(f"{label}: malformed contacts payload")
    return event.payload


async def resolve_channel(
    client: MeshCore, name: str, label: str
) -> tuple[int, bytes]:
    """Resolve one exact channel without printing or comparing its secret."""
    device = require_event(
        await client.commands.send_device_query(),
        EventType.DEVICE_INFO,
        f"{label} device query",
    )
    matches: list[tuple[int, bytes]] = []
    for index in range(device.payload["max_channels"]):
        event = await client.commands.get_channel(index)
        if event.type == EventType.CHANNEL_INFO and event.payload.get(
            "channel_name"
        ) == name:
            matches.append((index, event.payload["channel_secret"]))
    if len(matches) != 1:
        raise AssertionError(
            f"{label}: expected exactly one channel named {name!r}, "
            f"found {len(matches)}"
        )
    return matches[0]


async def send_dm(
    client: MeshCore,
    destination: dict[str, Any],
    marker: str,
    label: str,
    budget: SendBudget,
) -> tuple[str, float]:
    """Send one attempt-zero DM and return its ACK token and wait deadline."""
    budget.consume(label)
    event = require_event(
        await client.commands.send_msg(
            destination, marker, timestamp=int(time.time()), attempt=0
        ),
        EventType.MSG_SENT,
        label,
    )
    token = event.payload["expected_ack"].hex()
    timeout = max(5.0, event.payload["suggested_timeout"] / 1000 * 1.25 + 1)
    emit(
        "dm_accepted",
        label=label,
        marker_sha256=marker_digest(marker),
        suggested_timeout_ms=event.payload["suggested_timeout"],
    )
    return token, timeout


async def send_channel(
    client: MeshCore,
    channel_index: int,
    marker: str,
    label: str,
    budget: SendBudget,
) -> None:
    """Send one channel frame; native OK is acceptance, not delivery evidence."""
    budget.consume(label)
    require_event(
        await client.commands.send_chan_msg(
            channel_index, marker, timestamp=int(time.time())
        ),
        EventType.OK,
        label,
    )
    emit("channel_accepted", label=label, marker_sha256=marker_digest(marker))


async def receive_sequence(
    client: MeshCore,
    label: str,
    markers: list[str],
    expected_type: EventType,
    timeout: float,
    max_unmatched: int,
) -> None:
    """Pull exact markers in order while bounding consumption of other traffic."""
    deadline = asyncio.get_running_loop().time() + timeout
    unmatched = 0
    for expected in markers:
        while asyncio.get_running_loop().time() < deadline:
            remaining = deadline - asyncio.get_running_loop().time()
            try:
                event = await client.commands.get_msg(timeout=min(12, remaining))
            except asyncio.TimeoutError:
                await asyncio.sleep(0.25)
                continue
            if event is None or event.type == EventType.NO_MORE_MSGS:
                await asyncio.sleep(0.25)
                continue
            if event.type == EventType.ERROR:
                raise AssertionError(f"{label}: inbox error {event.payload!r}")
            if event.type not in INBOX_TYPES:
                raise AssertionError(
                    f"{label}: unexpected inbox event {event.type.value}"
                )
            text = event.payload.get("text")
            if event.type == expected_type and text == expected:
                emit(
                    "message_received",
                    endpoint=label,
                    kind=event.type.value,
                    marker_sha256=marker_digest(expected),
                    unmatched_before_marker=unmatched,
                )
                break
            unmatched += 1
            if unmatched > max_unmatched:
                raise AssertionError(
                    f"{label}: exceeded --max-unmatched while waiting for test marker"
                )
        else:
            raise AssertionError(
                f"{label}: timed out waiting for marker {marker_digest(expected)}"
            )


async def short_query_rounds(
    clients: dict[str, MeshCore], rounds: int
) -> None:
    """Exercise shared upstream ownership without adding radio traffic."""
    for iteration in range(rounds):
        events = await asyncio.gather(
            *(client.commands.get_time() for client in clients.values())
        )
        for label, event in zip(clients, events):
            require_event(
                event,
                EventType.CURRENT_TIME,
                f"{label} concurrent clock query {iteration}",
            )
    emit("concurrency_complete", rounds=rounds, clients=len(clients))


def make_marker(run_id: str, sequence: int, purpose: str) -> str:
    """Create a short, unique synthetic body that remains within DM limits."""
    return f"mux-live {run_id} {sequence:02d} {purpose}"


async def run(args: argparse.Namespace) -> dict[str, Any]:
    """Run the selected bounded check and return its machine-readable summary."""
    active: list[MeshCore] = []
    budget = SendBudget(args.max_radio_sends)
    run_id = f"{int(time.time())}-{secrets.token_hex(3)}"
    sequence = 0

    def marker(purpose: str) -> str:
        nonlocal sequence
        sequence += 1
        return make_marker(run_id, sequence, purpose)

    async def open_client(host: str, port: int, label: str) -> MeshCore:
        client = await connect(host, port, args.timeout, label)
        active.append(client)
        emit(
            "connected",
            endpoint=label,
            identity_prefix=client.self_info["public_key"][:12],
        )
        return client

    async def close_client(client: MeshCore) -> None:
        await client.disconnect()
        if client in active:
            active.remove(client)

    try:
        multi = await open_client(args.mux_host, args.multi_port, "multi")
        dedicated: list[MeshCore] = []
        for index, port in enumerate(args.dedicated_ports, 1):
            dedicated.append(
                await open_client(args.mux_host, port, f"dedicated-{index}")
            )
        peer = await open_client(args.peer_host, args.peer_port, "peer")

        mux_key = multi.self_info["public_key"]
        peer_key = peer.self_info["public_key"]
        if any(client.self_info["public_key"] != mux_key for client in dedicated):
            raise AssertionError("mux listeners exposed different identities")
        if peer_key == mux_key:
            raise AssertionError("peer and mux companion identities unexpectedly match")

        mux_contacts = await get_contacts(multi, "multi")
        peer_contacts = await get_contacts(peer, "peer")
        peer_destination = resolve_contact(
            mux_contacts, args.peer_contact, peer_key, "multi"
        )
        mux_destination = resolve_contact(
            peer_contacts, args.mux_contact, mux_key, "peer"
        )
        peer_channel_index: int | None = None
        if args.channel:
            mux_channel_index, mux_channel_secret = await resolve_channel(
                multi, args.channel, "multi"
            )
            peer_channel_index, peer_channel_secret = await resolve_channel(
                peer, args.channel, "peer"
            )
            if mux_channel_secret != peer_channel_secret:
                raise AssertionError(
                    f"channel {args.channel!r} has different secrets on mux and peer"
                )
            emit(
                "channel_preflight_complete",
                mux_channel_index=mux_channel_index,
                peer_channel_index=peer_channel_index,
            )
        emit(
            "preflight_complete",
            mux_identity_prefix=mux_key[:12],
            peer_identity_prefix=peer_key[:12],
            mux_contacts=len(mux_contacts),
            peer_contacts=len(peer_contacts),
            execute=args.execute,
        )

        shared_clients = {"multi": multi}
        shared_clients.update(
            {
                f"dedicated-{index}": client
                for index, client in enumerate(dedicated, 1)
            }
        )
        await short_query_rounds(shared_clients, args.query_rounds)
        if not args.execute:
            return {
                "status": "ok",
                "execute": False,
                "connections": len(active),
                "query_rounds": args.query_rounds,
                "radio_sends": 0,
            }

        ack = AckCollector()
        for label, client in shared_clients.items():
            ack.attach(label, client)
        ack.attach("peer", peer)
        ack_labels = list(shared_clients)

        # Each listener independently sends one real DM. Actual receipt at the
        # peer is the primary evidence; matching ACK broadcast is also required.
        for label, sender in shared_clients.items():
            body = marker(f"{label}-to-peer")
            token, ack_timeout = await send_dm(
                sender, peer_destination, body, f"{label} DM", budget
            )
            await receive_sequence(
                peer,
                "peer",
                [body],
                EventType.CONTACT_MSG_RECV,
                args.receive_timeout,
                args.max_unmatched,
            )
            await ack.require_token(token, ack_labels, ack_timeout)
            emit("ack_fanout_verified", sender=label, clients=len(ack_labels))

        # One inbound physical pop must become independent copies for the live
        # multi-client and every configured dedicated-client slot.
        inbound = marker("peer-to-all")
        peer_token, peer_ack_timeout = await send_dm(
            peer, mux_destination, inbound, "peer fanout DM", budget
        )
        await receive_sequence(
            multi,
            "multi",
            [inbound],
            EventType.CONTACT_MSG_RECV,
            args.receive_timeout,
            args.max_unmatched,
        )
        await ack.require_token(peer_token, ["peer"], peer_ack_timeout)
        for index, client in enumerate(dedicated, 1):
            await receive_sequence(
                client,
                f"dedicated-{index}",
                [inbound],
                EventType.CONTACT_MSG_RECV,
                args.receive_timeout,
                args.max_unmatched,
            )
        emit("live_fanout_verified", recipients=1 + len(dedicated))

        # Detach every dedicated client. Multi-client pulls transfer custody and
        # must populate both stable port queues for later FIFO backfill.
        for client in list(dedicated):
            await close_client(client)
        dedicated = []
        retained = [marker("retained-1"), marker("retained-2")]
        for body in retained:
            peer_token, peer_ack_timeout = await send_dm(
                peer, mux_destination, body, "peer retained DM", budget
            )
            await receive_sequence(
                multi,
                "multi",
                [body],
                EventType.CONTACT_MSG_RECV,
                args.receive_timeout,
                args.max_unmatched,
            )
            await ack.require_token(peer_token, ["peer"], peer_ack_timeout)

        for index, port in enumerate(args.dedicated_ports, 1):
            client = await open_client(
                args.mux_host, port, f"dedicated-{index}-reconnected"
            )
            dedicated.append(client)
            await receive_sequence(
                client,
                f"dedicated-{index}-reconnected",
                retained,
                EventType.CONTACT_MSG_RECV,
                args.receive_timeout,
                args.max_unmatched,
            )
        emit(
            "detached_backfill_verified",
            dedicated_ports=len(dedicated),
            messages=len(retained),
        )

        # Build one unread entry, attach A, then let B replace it on the same
        # dedicated port. B must inherit only the port-owned queue, not A's
        # transient connection state.
        await close_client(dedicated[0])
        takeover_marker = marker("takeover")
        peer_token, peer_ack_timeout = await send_dm(
            peer, mux_destination, takeover_marker, "peer takeover DM", budget
        )
        await receive_sequence(
            multi,
            "multi",
            [takeover_marker],
            EventType.CONTACT_MSG_RECV,
            args.receive_timeout,
            args.max_unmatched,
        )
        await ack.require_token(peer_token, ["peer"], peer_ack_timeout)
        takeover_a = await open_client(
            args.mux_host, args.dedicated_ports[0], "takeover-a"
        )
        disconnected: asyncio.Queue[Any] = asyncio.Queue()

        async def connection_closed(event: Any) -> None:
            await disconnected.put(event.payload)

        takeover_a.subscribe(EventType.DISCONNECTED, connection_closed)
        takeover_b = await open_client(
            args.mux_host, args.dedicated_ports[0], "takeover-b"
        )
        await asyncio.wait_for(disconnected.get(), 5)
        if takeover_a.is_connected:
            raise AssertionError("replaced dedicated client still reports connected")
        await receive_sequence(
            takeover_b,
            "takeover-b",
            [takeover_marker],
            EventType.CONTACT_MSG_RECV,
            args.receive_timeout,
            args.max_unmatched,
        )
        # Other dedicated ports receive the same arrival and remain usable.
        for index, client in enumerate(dedicated[1:], 2):
            await receive_sequence(
                client,
                f"dedicated-{index}-reconnected",
                [takeover_marker],
                EventType.CONTACT_MSG_RECV,
                args.receive_timeout,
                args.max_unmatched,
            )
        dedicated[0] = takeover_b
        emit("dedicated_takeover_verified", port_index=1)

        channel_sends = 0
        if args.channel:
            if peer_channel_index is None:
                raise AssertionError("channel preflight did not select a peer index")
            channel_marker = marker("channel-fanout")
            await send_channel(
                peer,
                peer_channel_index,
                channel_marker,
                "peer channel fanout",
                budget,
            )
            channel_sends = 1
            await receive_sequence(
                multi,
                "multi",
                [channel_marker],
                EventType.CHANNEL_MSG_RECV,
                args.receive_timeout,
                args.max_unmatched,
            )
            for index, client in enumerate(dedicated, 1):
                await receive_sequence(
                    client,
                    f"dedicated-{index}",
                    [channel_marker],
                    EventType.CHANNEL_MSG_RECV,
                    args.receive_timeout,
                    args.max_unmatched,
                )
            emit("channel_fanout_verified", recipients=1 + len(dedicated))

        # End with a fresh multi-client socket and a real read-only response so
        # repeated connect/disconnect behavior is part of the acceptance gate.
        await close_client(multi)
        multi = await open_client(args.mux_host, args.multi_port, "multi-reconnected")
        require_event(
            await multi.commands.get_time(),
            EventType.CURRENT_TIME,
            "reconnected multi clock query",
        )
        emit("multi_reconnect_verified")

        return {
            "status": "ok",
            "execute": True,
            "dedicated_ports": len(args.dedicated_ports),
            "query_rounds": args.query_rounds,
            "dm_sends": budget.used - channel_sends,
            "channel_sends": channel_sends,
            "radio_sends": budget.used,
            "backfill_messages_per_dedicated": len(retained),
            "takeovers": 1,
            "meshcore_py": importlib.metadata.version("meshcore"),
            "meshcore_cli": importlib.metadata.version("meshcore-cli"),
        }
    finally:
        await asyncio.gather(
            *(client.disconnect() for client in active), return_exceptions=True
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse explicit endpoints and reject unsafe or unbounded settings."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mux-host", required=True)
    parser.add_argument("--multi-port", required=True, type=int)
    parser.add_argument(
        "--dedicated-port",
        dest="dedicated_ports",
        required=True,
        action="append",
        type=int,
        help="Dedicated listener port; repeat for independent port identities",
    )
    parser.add_argument("--peer-host", required=True)
    parser.add_argument("--peer-port", required=True, type=int)
    parser.add_argument("--mux-contact", required=True)
    parser.add_argument("--peer-contact", required=True)
    parser.add_argument(
        "--channel",
        help="Optional exact private channel name for one fan-out message",
    )
    parser.add_argument("--query-rounds", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--receive-timeout", type=float, default=90)
    parser.add_argument("--max-unmatched", type=int, default=4)
    parser.add_argument("--max-radio-sends", type=int, default=12)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    ports = [args.multi_port, *args.dedicated_ports, args.peer_port]
    if any(not 1 <= port <= 65535 for port in ports):
        parser.error("all ports must be between 1 and 65535")
    if len(set([args.multi_port, *args.dedicated_ports])) != 1 + len(
        args.dedicated_ports
    ):
        parser.error("mux listener ports must be unique")
    if not args.dedicated_ports:
        parser.error("provide at least one --dedicated-port")
    if args.query_rounds < 1:
        parser.error("--query-rounds must be at least one")
    if args.timeout <= 0 or args.receive_timeout <= 0:
        parser.error("timeouts must be positive")
    if args.max_unmatched < 0:
        parser.error("--max-unmatched must not be negative")
    if args.max_radio_sends < 1:
        parser.error("--max-radio-sends must be at least one")
    return args


def main() -> int:
    """Run the harness and return a conventional process status."""
    try:
        result = asyncio.run(run(parse_args()))
    except Exception as exc:
        print(f"check_live_dedicated: FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
