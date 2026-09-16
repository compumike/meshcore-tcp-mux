"""Offline safety and correlation tests for the dedicated live harness."""

import contextlib
import io
import json
import unittest
from types import SimpleNamespace

from meshcore import EventType

from check_live_dedicated import (
    SendBudget,
    inbox_text_matches,
    make_marker,
    marker_digest,
    parse_args,
    receive_sequence,
    resolve_contact,
)


class LiveDedicatedHarnessTests(unittest.TestCase):
    """Keep live destination fencing and traffic limits fail-closed."""

    def test_contact_requires_both_exact_name_and_full_identity(self):
        contacts = {
            "aa" * 32: {
                "public_key": "aa" * 32,
                "adv_name": "Synthetic Peer",
            },
            "bb" * 32: {
                "public_key": "bb" * 32,
                "adv_name": "Synthetic Peer",
            },
        }

        selected = resolve_contact(
            contacts, "synthetic peer", "bb" * 32, "test endpoint"
        )
        self.assertEqual(selected["public_key"], "bb" * 32)
        with self.assertRaises(AssertionError):
            resolve_contact(
                contacts, "different peer", "bb" * 32, "test endpoint"
            )

    def test_send_budget_fails_before_an_extra_transmission(self):
        budget = SendBudget(2)
        budget.consume("first")
        budget.consume("second")
        with self.assertRaises(AssertionError):
            budget.consume("third")
        self.assertEqual(budget.used, 2)

    def test_report_digest_does_not_contain_marker_body(self):
        marker = make_marker("synthetic-run", 1, "test-purpose")
        digest = marker_digest(marker)
        self.assertEqual(len(digest), 64)
        self.assertNotIn(marker, digest)

    def test_channel_matcher_accounts_for_firmware_sender_prefix(self):
        marker = "mux-live synthetic 01 channel-fanout"
        self.assertTrue(
            inbox_text_matches(
                EventType.CHANNEL_MSG_RECV,
                f"Synthetic Node: {marker}",
                marker,
            )
        )
        self.assertTrue(
            inbox_text_matches(
                EventType.CHANNEL_MSG_RECV,
                f"Node: With Colon: {marker}",
                marker,
            )
        )
        self.assertFalse(
            inbox_text_matches(EventType.CHANNEL_MSG_RECV, marker, marker)
        )
        self.assertFalse(
            inbox_text_matches(
                EventType.CHANNEL_MSG_RECV,
                f"Synthetic Node: {marker} trailing",
                marker,
            )
        )

    def test_direct_matcher_still_requires_the_exact_body(self):
        marker = "mux-live synthetic 01 direct"
        self.assertTrue(
            inbox_text_matches(EventType.CONTACT_MSG_RECV, marker, marker)
        )
        self.assertFalse(
            inbox_text_matches(
                EventType.CONTACT_MSG_RECV,
                f"Synthetic Node: {marker}",
                marker,
            )
        )

    def test_parser_accepts_one_dedicated_port_and_rejects_duplicates(self):
        common = [
            "--mux-host",
            "mux.invalid",
            "--multi-port",
            "5001",
            "--peer-host",
            "peer.invalid",
            "--peer-port",
            "6001",
            "--mux-contact",
            "Synthetic Mux",
            "--peer-contact",
            "Synthetic Peer",
        ]
        parsed = parse_args([*common, "--dedicated-port", "5002"])
        self.assertEqual(parsed.dedicated_ports, [5002])
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parse_args(
                    [
                        *common,
                        "--dedicated-port",
                        "5002",
                        "--dedicated-port",
                        "5002",
                    ]
                )


class SequenceEvidenceTests(unittest.IsolatedAsyncioTestCase):
    """Verify inbox failures retain safe evidence and detect immediate duplicates."""

    @staticmethod
    def client_with(*events):
        """Build the minimum async command surface used by receive_sequence."""
        remaining = list(events)

        async def get_msg(timeout):
            del timeout
            return remaining.pop(0)

        return SimpleNamespace(commands=SimpleNamespace(get_msg=get_msg))

    @staticmethod
    def inbox_event(text: str):
        """Construct a decoded CONTACT_MSG_RECV event without a radio connection."""
        return SimpleNamespace(
            type=EventType.CONTACT_MSG_RECV,
            payload={"text": text},
        )

    async def test_strict_unmatched_failure_emits_kind_and_hash_without_body(self):
        expected = "mux-live synthetic expected"
        unexpected = "private unrelated test fixture"
        client = self.client_with(self.inbox_event(unexpected))
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            with self.assertRaisesRegex(AssertionError, "exceeded --max-unmatched"):
                await receive_sequence(
                    client,
                    "synthetic-peer",
                    [expected],
                    EventType.CONTACT_MSG_RECV,
                    timeout=1,
                    max_unmatched=0,
                )

        record = json.loads(output.getvalue())
        self.assertEqual(record["event"], "message_unmatched")
        self.assertEqual(record["kind"], EventType.CONTACT_MSG_RECV.value)
        self.assertEqual(record["content_sha256"], marker_digest(unexpected))
        self.assertNotIn(unexpected, output.getvalue())

    async def test_optional_empty_check_rejects_trailing_duplicate(self):
        marker = "mux-live synthetic once"
        # Two identical decoded contact messages model an immediate duplicate
        # after the single expected attempt-zero transmission.
        client = self.client_with(
            self.inbox_event(marker),
            self.inbox_event(marker),
        )
        output = io.StringIO()

        with contextlib.redirect_stdout(output):
            with self.assertRaisesRegex(AssertionError, "expected empty inbox"):
                await receive_sequence(
                    client,
                    "synthetic-peer",
                    [marker],
                    EventType.CONTACT_MSG_RECV,
                    timeout=1,
                    max_unmatched=0,
                    verify_empty_after=True,
                )

        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(records[-1]["event"], "message_unmatched")
        self.assertEqual(records[-1]["reason"], "after_expected_sequence")
        self.assertEqual(records[-1]["content_sha256"], marker_digest(marker))


if __name__ == "__main__":
    unittest.main()
