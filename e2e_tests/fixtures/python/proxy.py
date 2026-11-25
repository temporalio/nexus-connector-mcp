"""Worker with the outbound proxy plugin.

Prints READY when the Worker runs.
"""

import argparse
import asyncio
import os

from mcp import Client

from stdin_watch import exit_on_stdin_close
from temporalio.client import Client as TemporalClient
from temporalio.nexus_mcp.outbound import MCPProxyPlugin, Options, Upstream
from temporalio.worker import Worker


async def main() -> None:
    exit_on_stdin_close()
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-queue", required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--upstream", required=True)
    args = parser.parse_args()

    client = await TemporalClient.connect(
        os.environ["TEMPORAL_ADDRESS"], namespace=os.environ["TEMPORAL_NAMESPACE"]
    )
    plugin = MCPProxyPlugin(
        options=Options(
            upstreams=[
                Upstream(
                    service=args.service, client_factory=lambda: Client(args.upstream)
                )
            ]
        )
    )
    async with Worker(client, task_queue=args.task_queue, plugins=[plugin]):
        print("READY", flush=True)
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
