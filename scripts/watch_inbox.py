"""Read-only live receive check using two independent meshcore_py sessions.

Run with the meshcore-cli virtual environment's Python. No messages are sent.
Only hashes/counts of received payloads are printed, never message contents.
"""
import argparse
import asyncio
from collections import Counter
import hashlib
import json
import time

from meshcore import MeshCore, EventType


async def run(args):
    clients = await asyncio.gather(*(
        MeshCore.create_tcp(args.host, args.port) for _ in range(2)
    ))
    observed = [Counter(), Counter()]
    kinds = [set(), set()]
    required = {
        "both": {EventType.CONTACT_MSG_RECV, EventType.CHANNEL_MSG_RECV},
        "dm": {EventType.CONTACT_MSG_RECV},
        "channel": {EventType.CHANNEL_MSG_RECV},
    }[args.require]
    ready = asyncio.Event()

    async def read(index):
        await ready.wait()
        deadline = time.monotonic() + args.seconds
        while time.monotonic() < deadline:
            event = await clients[index].commands.get_msg()
            if event.type == EventType.ERROR:
                raise AssertionError(f"client {index + 1}: inbox returned an error")
            if event.type in (EventType.CONTACT_MSG_RECV, EventType.CHANNEL_MSG_RECV):
                canonical = json.dumps(event.payload, sort_keys=True, default=str).encode()
                digest = hashlib.sha256(canonical).hexdigest()
                observed[index][(event.type.value, digest)] += 1
                kinds[index].add(event.type)
                print(f"client={index + 1} kind={event.type.value} sha256={digest}", flush=True)
            if all(required <= k for k in kinds) and observed[0] == observed[1]:
                return
            await asyncio.sleep(0.3)

    try:
        tasks = [asyncio.create_task(read(i)) for i in range(2)]
        print(f"LISTENING: two clients connected; required={args.require}", flush=True)
        ready.set()
        await asyncio.gather(*tasks)
        assert all(required <= k for k in kinds), f"live {args.require} receive evidence is incomplete"
        assert observed[0] == observed[1], "clients did not receive identical message multisets"
        print(f"PASS: identical live {args.require} copies; items_per_client={sum(observed[0].values())}", flush=True)
    finally:
        await asyncio.gather(*(client.disconnect() for client in clients))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("--seconds", type=float, default=180)
    parser.add_argument("--require", choices=("both", "dm", "channel"), default="both")
    asyncio.run(run(parser.parse_args()))
