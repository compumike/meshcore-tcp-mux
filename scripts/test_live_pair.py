"""Offline argument fencing and partial-ACK diagnosis for the live pair harness."""
import contextlib
import io
import unittest

from check_live_dedicated import AckCollector
from check_live_pair import InboxWireClient, parse_args


class LivePairArgumentsTests(unittest.TestCase):
    """Keep explicitly selected physical endpoints and bounded traffic controls."""

    def arguments(self):
        return ["--a-host", "companion-a.test", "--a-port", "5000",
                "--b-host", "companion-b.test", "--b-port", "5050",
                "--channel", "#synthetic"]

    def test_default_never_sends_radio_messages_or_injects_disconnects(self):
        args = parse_args(self.arguments())
        self.assertFalse(args.execute)
        self.assertFalse(args.reconnect)
        self.assertEqual(args.stress_rounds, 15)

    def test_rejects_same_physical_endpoint(self):
        arguments = self.arguments()
        arguments[6] = "companion-a.test"
        arguments[8] = "5000"
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parse_args(arguments)

    def test_rejects_invalid_ports_and_unbounded_rounds(self):
        for option, value in (("--a-port", "0"), ("--b-port", "65536"),
                              ("--stress-rounds", "0"), ("--stress-rounds", "101")):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_args(self.arguments() + [option, value])


class AckDiagnosisTests(unittest.IsolatedAsyncioTestCase):
    """Distinguish a missing physical ACK from only one client losing its copy."""

    async def test_timeout_names_only_sessions_without_a_matching_ack(self):
        import asyncio
        collector = AckCollector()
        collector.queues = {label: asyncio.Queue() for label in ("owner", "observer")}
        # Synthetic ACK CRC, unrelated to any real contact, key, or message body.
        await collector.queues["owner"].put({"code": "aabbccdd", "trip_time": 123})
        with self.assertRaisesRegex(AssertionError, "matching ACK missing on observer after"):
            await collector.require_token("aabbccdd", ["owner", "observer"], 0.02)

    async def test_all_observers_must_receive_identical_matching_ack_payloads(self):
        import asyncio
        collector = AckCollector()
        collector.queues = {label: asyncio.Queue() for label in ("owner", "observer")}
        # Same synthetic CRC with contradictory trip times is not identical fanout.
        for label, trip in (("owner", 123), ("observer", 456)):
            await collector.queues[label].put({"code": "aabbccdd", "trip_time": trip})
        with self.assertRaisesRegex(AssertionError, "different ACK payloads"):
            await collector.require_token("aabbccdd", ["owner", "observer"], 0.1)


class InboxWireClientTests(unittest.IsolatedAsyncioTestCase):
    """Keep raw dialect evidence independent of SDK parsing and push timing."""

    async def test_reply_skips_availability_hints_before_the_owned_clock_reply(self):
        import asyncio
        reader = asyncio.StreamReader()
        # '>' envelope with LE lengths: MSG_WAITING (0x83), followed by
        # CURRENT_TIME (9) and synthetic four-byte LE timestamp 1.
        reader.feed_data(b">\x01\x00\x83>\x05\x00\x09\x01\x00\x00\x00")
        reader.feed_eof()
        client = InboxWireClient(reader, None)
        self.assertEqual(await client.reply(), b"\x09\x01\x00\x00\x00")

    async def test_reply_rejects_invalid_envelopes_without_allocating_announced_bodies(self):
        import asyncio
        # Wrong direction, zero payload, and payload 177 beyond the known limit.
        for header in (b"<\x01\x00", b">\x00\x00", b">\xb1\x00"):
            reader = asyncio.StreamReader()
            reader.feed_data(header)
            reader.feed_eof()
            client = InboxWireClient(reader, None)
            with self.assertRaisesRegex(AssertionError, "invalid raw downstream envelope"):
                await client.reply()


if __name__ == "__main__":
    unittest.main()
