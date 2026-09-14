"""Exercise the live harness's readiness parser without contacting a companion."""

import asyncio
from collections import deque
import unittest

from check_live_reconnect import read_mux_stderr


class ReadinessTests(unittest.IsolatedAsyncioTestCase):
    """Keep log-format changes from silently disabling live validation gates."""

    async def test_current_events_preserve_epochs_and_ignore_other_lines(self):
        stream = asyncio.StreamReader()
        lines = [
            "INFO event=upstream.connecting epoch=1",
            "INFO event=upstream.ready epoch=2 remote=127.0.0.1 profile=native_v13",
            "INFO profile=native_v13 epoch=4 event=upstream.ready remote=127.0.0.1",
            "INFO event=upstream.ready epoch=invalid",
            "INFO event=upstream.ready epoch=0",
            "INFO event=upstream.ready",
        ]
        stream.feed_data(("\n".join(lines) + "\n").encode())
        stream.feed_eof()
        ready = asyncio.Queue()
        tail = deque(maxlen=3)
        await read_mux_stderr(stream, ready, tail)
        self.assertEqual([ready.get_nowait(), ready.get_nowait()], [2, 4])
        self.assertTrue(ready.empty())
        self.assertEqual(list(tail), lines[-3:])


if __name__ == "__main__":
    unittest.main()
