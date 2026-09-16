"""Offline safety and correlation tests for the dedicated live harness."""

import contextlib
import io
import unittest

from check_live_dedicated import (
    SendBudget,
    make_marker,
    marker_digest,
    parse_args,
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


if __name__ == "__main__":
    unittest.main()
