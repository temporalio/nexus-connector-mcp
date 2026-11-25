"""Sample Worker that proxies an upstream MCP server that requires OAuth.

This is a sample, not a library API. main.py uses the connector library.
oauth.py adds OAuth with the MCP SDK client credentials provider.

Run from the python directory:

    OAUTH_CLIENT_SECRET=... uv run ../samples/python/oauth_outbound_connector/main.py \
        --upstream https://mcp.example.com/mcp --issuer https://login.example.com \
        --client-id mcp-proxy --service weather
"""

import argparse
import asyncio

from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from oauth import add_oauth_arguments, oauth_http_client

from temporalio.client import Client as TemporalClient
from temporalio.nexus_mcp.outbound import MCPProxyPlugin, Options, Upstream
from temporalio.worker import Worker


async def main() -> None:
    """Run the proxy Worker."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--temporal", default="localhost:7233", help="Temporal frontend address"
    )
    parser.add_argument(
        "--namespace", default="default", help="Temporal namespace of the Worker"
    )
    parser.add_argument("--task-queue", default="mcp-proxy", help="Worker task queue")
    parser.add_argument("--service", required=True, help="Nexus service name")
    parser.add_argument(
        "--upstream",
        required=True,
        help="upstream MCP URL, such as https://mcp.example.com/mcp",
    )
    add_oauth_arguments(parser)
    args = parser.parse_args()

    client = await TemporalClient.connect(args.temporal, namespace=args.namespace)
    async with oauth_http_client(args, args.upstream) as http_client:
        plugin = MCPProxyPlugin(
            options=Options(
                upstreams=[
                    Upstream(
                        service=args.service,
                        client_factory=lambda: Client(
                            streamable_http_client(
                                args.upstream, http_client=http_client
                            )
                        ),
                    )
                ]
            )
        )
        worker = Worker(client, task_queue=args.task_queue, plugins=[plugin])
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
