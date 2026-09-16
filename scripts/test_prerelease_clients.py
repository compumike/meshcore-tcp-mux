"""Pre-release integration tests using real meshcore_py clients and a fake radio.

These tests cover the stable dedicated-port behavior that cannot be established
by calling the Crystal broker directly.  Every TCP peer below is a real
``MeshCore`` client; only the single upstream companion is simulated.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import unittest
from collections import deque
from pathlib import Path
from typing import Any

from meshcore import EventType, MeshCore

from check_fake_clients import (
    FakeCompanion,
    channel_message,
    connect,
    contact_message,
    reserve_port,
    require_event,
    stderr_reader,
    wait_until,
)


class GatedFakeCompanion(FakeCompanion):
    """Adds one controllable clock reply for replacement-during-command tests."""

    def __init__(self) -> None:
        super().__init__()
        self.block_next_time = False
        self.time_command_seen = asyncio.Event()
        self.release_time_response = asyncio.Event()

    async def command(self, payload: bytes) -> None:
        """Hold exactly one GET_DEVICE_TIME reply until the test releases it."""
        if payload and payload[0] == 5 and self.block_next_time:
            # GET_DEVICE_TIME (5) normally receives CURRENT_TIME (9) carrying
            # one unsigned little-endian 32-bit Unix timestamp at bytes 1..4.
            self.block_next_time = False
            self.commands[5] += 1
            self.time_value += 1
            self.time_command_seen.set()
            await self.release_time_response.wait()
            await self.send(bytes([9]) + self.time_value.to_bytes(4, "little"))
            return
        await super().command(payload)


class MuxHarness:
    """Owns one fake companion, one mux process, and isolated loopback ports."""

    def __init__(self, offline_queue_size: int = 8) -> None:
        self.fake = GatedFakeCompanion()
        self.multi_port = 0
        self.dedicated_port = 0
        self.offline_queue_size = offline_queue_size
        self.process: asyncio.subprocess.Process | None = None
        self.stderr_task: asyncio.Task[None] | None = None
        self.stderr_tail: deque[str] = deque(maxlen=80)
        self.clients: list[MeshCore] = []

    async def start(self) -> None:
        """Start the fake and wait for the candidate binary's ready event."""
        binary = Path("out/meshcore-tcp-mux").resolve()
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise AssertionError(f"mux binary is not executable: {binary}")
        await self.fake.start()
        ports = {self.fake.port}
        while len(ports) < 3:
            ports.add(reserve_port())
        self.multi_port, self.dedicated_port = sorted(ports - {self.fake.port})
        ready = asyncio.Event()
        self.process = await asyncio.create_subprocess_exec(
            str(binary),
            "--upstream-host",
            "127.0.0.1",
            "--upstream-port",
            str(self.fake.port),
            "--listen-host",
            "127.0.0.1",
            "--listen-multi-client-port",
            str(self.multi_port),
            "--listen-dedicated-client-port",
            str(self.dedicated_port),
            "--offline-queue-size",
            str(self.offline_queue_size),
            "--poll-interval",
            "60",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        assert self.process.stderr
        self.stderr_task = asyncio.create_task(
            stderr_reader(self.process.stderr, ready, self.stderr_tail)
        )
        try:
            await asyncio.wait_for(ready.wait(), timeout=6)
        except Exception as exc:
            raise AssertionError(self.failure_detail(str(exc))) from exc

    async def open(self, port: int, label: str) -> MeshCore:
        """Connect a tracked real client so teardown closes every session."""
        client = await connect(port, label)
        self.clients.append(client)
        return client

    async def close_client(self, client: MeshCore) -> None:
        """Disconnect one client while keeping teardown idempotent."""
        await client.disconnect()
        with contextlib.suppress(ValueError):
            self.clients.remove(client)

    async def close(self) -> None:
        """Stop all downstreams, the mux, and the fake companion."""
        await asyncio.gather(
            *(client.disconnect() for client in self.clients), return_exceptions=True
        )
        self.clients.clear()
        if self.process and self.process.returncode is None:
            self.process.send_signal(signal.SIGTERM)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.process.wait(), timeout=4)
        if self.process and self.process.returncode is None:
            self.process.kill()
            await self.process.wait()
        await self.fake.close()
        if self.stderr_task:
            await self.stderr_task

    def failure_detail(self, message: str) -> str:
        """Attach bounded mux diagnostics to an integration assertion."""
        tail = "\n".join(self.stderr_tail)
        return f"{message}\nmux stderr tail:\n{tail}" if tail else message


async def pull_text(client: MeshCore, expected_type: EventType, label: str) -> str:
    """Pull one virtualized inbox item and return its decoded text."""
    event = require_event(await client.commands.get_msg(timeout=4), expected_type, label)
    return event.payload["text"]


class PrereleaseClientTests(unittest.IsolatedAsyncioTestCase):
    """Exercise difficult dedicated-port state transitions through real clients."""

    async def test_detached_fifo_membership_takeover_and_overflow(self) -> None:
        """Prove stable history, connection scoping, takeover, and overflow order."""
        harness = MuxHarness(offline_queue_size=2)
        await harness.start()
        try:
            multi = await harness.open(harness.multi_port, "multi")

            # Populate the detached slot.  With a two-entry queue, the third
            # direct message evicts the older channel-class entry, while the
            # fourth channel arrival is discarded because no channel remains.
            fixtures = [
                (channel_message("evict-channel", 3001), EventType.CHANNEL_MSG_RECV),
                (contact_message("direct-one", 3002), EventType.CONTACT_MSG_RECV),
                (contact_message("direct-two", 3003), EventType.CONTACT_MSG_RECV),
                (channel_message("discard-channel", 3004), EventType.CHANNEL_MSG_RECV),
            ]
            for index, (payload, event_type) in enumerate(fixtures):
                await harness.fake.enqueue(payload)
                await pull_text(multi, event_type, f"multi arrival {index}")

            late_multi = await harness.open(harness.multi_port, "late-multi")
            require_event(
                await late_multi.commands.get_msg(timeout=4),
                EventType.NO_MORE_MSGS,
                "late multi-client historical inbox",
            )

            takeover_a = await harness.open(harness.dedicated_port, "takeover-a")
            disconnected: asyncio.Queue[Any] = asyncio.Queue()

            async def connection_closed(event: Any) -> None:
                await disconnected.put(event.payload)

            takeover_a.subscribe(EventType.DISCONNECTED, connection_closed)
            takeover_b = await harness.open(harness.dedicated_port, "takeover-b")
            await asyncio.wait_for(disconnected.get(), timeout=4)
            self.assertFalse(takeover_a.is_connected)

            texts = [
                await pull_text(
                    takeover_b, EventType.CONTACT_MSG_RECV, f"dedicated retained {i}"
                )
                for i in range(2)
            ]
            self.assertEqual(texts, ["direct-one", "direct-two"])
            require_event(
                await takeover_b.commands.get_msg(timeout=4),
                EventType.NO_MORE_MSGS,
                "dedicated queue empty",
            )
        except Exception as exc:
            self.fail(harness.failure_detail(str(exc)))
        finally:
            await harness.close()

    async def test_immediate_contacts_and_clock_queries_under_concurrency(self) -> None:
        """Stress listener registration with replies sent in the same event-loop turn."""
        harness = MuxHarness()
        # All contact streams are immediate.  This deliberately removes the
        # historical 20 ms fake delay that could conceal a meshcore_py waiter race.
        harness.fake.contact_delays.clear()
        await harness.start()
        try:
            clients = await asyncio.gather(
                harness.open(harness.multi_port, "multi-a"),
                harness.open(harness.multi_port, "multi-b"),
                harness.open(harness.dedicated_port, "dedicated"),
            )
            for iteration in range(100):
                # Rotate issuance order so the dedicated slot and both
                # connection-scoped sessions each become the first waiter.
                offset = iteration % len(clients)
                ordered_clients = clients[offset:] + clients[:offset]
                if iteration % 2:
                    calls = [client.commands.get_time() for client in ordered_clients]
                    expected = EventType.CURRENT_TIME
                else:
                    calls = [
                        client.commands.get_contacts(timeout=4)
                        for client in ordered_clients
                    ]
                    expected = EventType.CONTACTS
                events = await asyncio.wait_for(asyncio.gather(*calls), timeout=8)
                for client_index, event in enumerate(events):
                    require_event(
                        event,
                        expected,
                        f"round {iteration} client {client_index}",
                    )
                    if expected == EventType.CONTACTS:
                        self.assertEqual(len(event.payload), 2)
        except Exception as exc:
            self.fail(harness.failure_detail(str(exc)))
        finally:
            await harness.close()

    async def test_takeover_during_active_command_does_not_reassign_reply(self) -> None:
        """Keep an old command's delayed terminator away from its replacement."""
        harness = MuxHarness()
        await harness.start()
        try:
            old = await harness.open(harness.dedicated_port, "old-dedicated")
            disconnected: asyncio.Queue[Any] = asyncio.Queue()

            async def connection_closed(event: Any) -> None:
                await disconnected.put(event.payload)

            old.subscribe(EventType.DISCONNECTED, connection_closed)
            harness.fake.block_next_time = True
            old_query = asyncio.create_task(old.commands.get_time())
            await asyncio.wait_for(harness.fake.time_command_seen.wait(), timeout=4)

            # The replacement's APP_START queues behind the old transaction.
            # Admission must still close the old socket immediately.  Releasing
            # CURRENT_TIME (9) completes only the removed owner's transaction.
            replacement_task = asyncio.create_task(
                harness.open(harness.dedicated_port, "replacement")
            )
            await asyncio.wait_for(disconnected.get(), timeout=4)
            harness.fake.release_time_response.set()
            replacement = await asyncio.wait_for(replacement_task, timeout=4)
            await asyncio.gather(old_query, return_exceptions=True)

            event = require_event(
                await replacement.commands.get_time(),
                EventType.CURRENT_TIME,
                "replacement clock query",
            )
            self.assertEqual(event.payload["time"], harness.fake.time_value)
            self.assertEqual(harness.fake.commands[5], 2)
        except Exception as exc:
            self.fail(harness.failure_detail(str(exc)))
        finally:
            await harness.close()


if __name__ == "__main__":
    unittest.main()
