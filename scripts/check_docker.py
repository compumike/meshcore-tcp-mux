import argparse
import asyncio
import json
import uuid

from check_fake_clients import (
    EventType, FakeCompanion, channel_message, connect, contact_message,
    require_event, wait_until,
)


async def docker(*args):
    # Use the caller's Docker context; never invoke a shell or publish an image.
    process = await asyncio.create_subprocess_exec(
        "docker", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    if process.returncode:
        raise RuntimeError(f"docker {args[0]} failed: {stderr.decode().strip()}")
    return stdout.decode().strip()


async def run(image):
    # Linux Docker Engine integration check. A unique bridge gives the fake
    # companion a host-side gateway address without exposing it on LAN interfaces.
    # The real image connects outward; clients enter through a published port.
    name = "meshcore-smoke-" + uuid.uuid4().hex[:12]
    network_created = False
    container_created = False
    fake = FakeCompanion()
    clients = []
    try:
        await docker("network", "create", name)
        network_created = True
        network = json.loads(await docker("network", "inspect", name))[0]
        gateway = network["IPAM"]["Config"][0]["Gateway"]
        fake.server = await asyncio.start_server(fake.handle, gateway, 0)
        await docker(
            "create", "--pull", "never", "--name", name, "--network", name,
            "--read-only", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges:true",
            "-p", "127.0.0.1::5001", "-p", "127.0.0.1::5002", image,
            "--upstream-host", gateway, "--upstream-port", str(fake.port),
            "--listen-host", "0.0.0.0", "--listen-multi-client-port", "5001",
            "--listen-dedicated-client-port", "5002",
        )
        container_created = True
        # Track creation before starting so a failed start still removes the
        # container and releases its network attachment in the finally block.
        await docker("start", name)
        # Startup ends with SET_FLOOD_SCOPE_KEY (54), restoring the physical
        # default scope before the mux admits downstream clients.
        await wait_until(lambda: fake.commands[54] > 0, 8, "container startup fence")
        info = json.loads(await docker("inspect", name))[0]
        assert info["Config"]["User"] == "10001:10001", "runtime must be non-root"
        port = int(info["NetworkSettings"]["Ports"]["5001/tcp"][0]["HostPort"])
        dedicated_port = int(info["NetworkSettings"]["Ports"]["5002/tcp"][0]["HostPort"])
        # Connect sequentially so a partial failure still leaves every client
        # registered for cleanup. Both remain connected during all assertions.
        for label in ("docker-a", "docker-b"):
            clients.append(await connect(port, label))
        clocks = await asyncio.gather(*(c.commands.get_time() for c in clients))
        for event in clocks:
            require_event(event, EventType.CURRENT_TIME, "container clock query")
        await fake.enqueue(contact_message("docker-dm"), channel_message("docker-channel"))
        await wait_until(lambda: not fake.offline, 4, "container inbox fanout")
        for kind, body in (
            (EventType.CONTACT_MSG_RECV, "docker-dm"),
            (EventType.CHANNEL_MSG_RECV, "docker-channel"),
        ):
            events = await asyncio.gather(*(c.commands.get_msg(timeout=4) for c in clients))
            for event in events:
                require_event(event, kind, "container inbox")
                assert event.payload["text"] == body, "container changed message body"
        # The dedicated client was detached while multi-client sessions drained
        # the companion. Its port-identified queue must backfill both messages.
        dedicated = await connect(dedicated_port, "docker-dedicated")
        clients.append(dedicated)
        for kind, body in (
            (EventType.CONTACT_MSG_RECV, "docker-dm"),
            (EventType.CHANNEL_MSG_RECV, "docker-channel"),
        ):
            event = await dedicated.commands.get_msg(timeout=4)
            require_event(event, kind, "dedicated reconnect backfill")
            assert event.payload["text"] == body, "dedicated backfill changed message body"
        # Stop while clients and upstream are connected. Exit zero proves the
        # process handled SIGTERM, rather than Docker falling back to SIGKILL.
        await docker("stop", "--time", "10", name)
        info = json.loads(await docker("inspect", name))[0]
        assert info["State"]["ExitCode"] == 0, "container did not stop gracefully"
        print(json.dumps({"status": "ok", "clients": 3, "fanout_items": 2,
                          "dedicated_backfill_items": 2,
                          "bridge_network": True, "read_only": True,
                          "non_root": True, "sigterm_exit_code": 0}))
    finally:
        await asyncio.gather(*(c.disconnect() for c in clients), return_exceptions=True)
        try:
            if container_created:
                await docker("rm", "-f", name)
        finally:
            await fake.close()
            if network_created:
                await docker("network", "rm", name)


def main():
    parser = argparse.ArgumentParser(
        description="Test a local image against a fake companion on Linux Docker Engine."
    )
    parser.add_argument("--image", default="compumike/meshcore-tcp-mux:latest")
    asyncio.run(run(parser.parse_args().image))


if __name__ == "__main__":
    main()
