"""One bounded live send check to explicitly selected destinations, without retries.

Resolves the exact permitted destinations first. Without --execute it is
read-only. A run sends one DM and one channel message; it never changes radio
configuration or chooses a replacement path after an error or missing ACK.
"""
import argparse
import asyncio
import json
import time

from meshcore import MeshCore, EventType
from check_clients import connect, require_event


async def run(args):
    clients = await asyncio.gather(*(connect(args.host, args.port, 8, str(i)) for i in range(2)))
    sender, observer = clients
    acknowledgements = [asyncio.Queue(), asyncio.Queue()]
    try:
        contacts = require_event(await sender.commands.get_contacts(timeout=10), EventType.CONTACTS, "contacts")
        permitted = [c for c in contacts.payload.values() if c.get("adv_name", "").casefold() == args.contact.casefold()]
        assert len(permitted) == 1, "expected exactly one contact matching --contact"
        device = require_event(await sender.commands.send_device_query(), EventType.DEVICE_INFO, "device query")
        channels = []
        for index in range(device.payload["max_channels"]):
            event = await sender.commands.get_channel(index)
            if event.type == EventType.CHANNEL_INFO and event.payload.get("channel_name") == args.channel:
                channels.append(index)
        assert len(channels) == 1, "expected exactly one channel matching --channel"
        print(json.dumps({"resolved": True, "channel_index": channels[0], "execute": args.execute}), flush=True)
        if not args.execute:
            return

        for index, client in enumerate(clients):
            async def ack(event, queue=acknowledgements[index]):
                await queue.put(event.payload)
            client.subscribe(EventType.ACK, ack)

        message = f"Hello from meshcore-tcp-mux #{args.sequence}"
        timestamp = int(time.time())
        # Read a local query on the other connection while the DM is accepted.
        result, query = await asyncio.gather(
            sender.commands.send_msg(permitted[0], message, timestamp=timestamp, attempt=0),
            observer.commands.get_time(),
        )
        require_event(query, EventType.CURRENT_TIME, "concurrent clock query")
        require_event(result, EventType.MSG_SENT, "DM acceptance")
        token = result.payload["expected_ack"].hex()
        print(json.dumps({"dm_accepted": True, "sequence": args.sequence, "suggested_timeout_ms": result.payload["suggested_timeout"]}), flush=True)

        # Independent channel send, with no retry even if a later ACK is lost.
        channel = await observer.commands.send_chan_msg(channels[0], message, timestamp=timestamp)
        require_event(channel, EventType.OK, "channel acceptance")
        print(json.dumps({"channel_accepted": True, "sequence": args.sequence}), flush=True)

        async def matching_ack(queue):
            while True:
                payload = await queue.get()
                if payload.get("code") == token:
                    return payload

        limit = max(5, result.payload["suggested_timeout"] / 1000 * 1.25 + 1)
        receipts = await asyncio.wait_for(asyncio.gather(*(matching_ack(q) for q in acknowledgements)), limit)
        assert receipts[0] == receipts[1], "clients received different confirmation payloads"
        print(json.dumps({"status": "ok", "dm_confirmed_on_both_clients": True, "round_trip_ms": receipts[0].get("trip_time"), "dm_sends": 1, "channel_sends": 1, "sequence": args.sequence}), flush=True)
    finally:
        await asyncio.gather(*(client.disconnect() for client in clients), return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("--contact", required=True, help="Exact contact name (case-insensitive)")
    parser.add_argument("--channel", required=True, help="Exact channel name, including any # prefix")
    parser.add_argument("--sequence", type=int, choices=range(1, 11), required=True)
    parser.add_argument("--execute", action="store_true")
    asyncio.run(run(parser.parse_args()))
