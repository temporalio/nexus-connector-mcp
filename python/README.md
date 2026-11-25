# temporalio-nexus-mcp

Python package for the Nexus MCP connector. Import path: `temporalio.nexus_mcp`.

> [!NOTE]
> This project is currently experimental. APIs are subject to change.

## Features

| Feature | Module | Entry points |
| --- | --- | --- |
| Authoring: marks Nexus operations as MCP tools. | `temporalio.nexus_mcp` | `service`. Safe to import in Workflow code. |
| | `temporalio.nexus_mcp.handler` | `service_handler`, `tool`. Nexus handler code only. |
| Workflow client: calls Nexus-backed MCP tools from Workflow code. | `temporalio.nexus_mcp.workflow` | `NexusMCPClient`. |
| Outbound proxy: runs upstream MCP tool calls as Activities behind a Nexus service. | `temporalio.nexus_mcp.outbound` | `MCPProxyPlugin`, `Options`, `Upstream`. |

The inbound connector is in the [Go module](../go). AI SDK bindings for `NexusMCPClient` are in
[temporalio/ai-integrations](https://github.com/temporalio/ai-integrations).

## Proxy upstream MCP servers

`MCPProxyPlugin` is a Worker plugin that proxies upstream MCP servers. Each `Upstream` becomes a
Nexus service on the Worker. Each tool call runs as a Standalone Activity, so Temporal retries,
times out, and records it.

The Temporal server must have Standalone Activities enabled.

```python
from datetime import timedelta

from mcp import Client
from temporalio.client import Client as TemporalClient
from temporalio.nexus_mcp.outbound import MCPProxyPlugin, Options, Upstream
from temporalio.worker import Worker


async def main() -> None:
    client = await TemporalClient.connect("localhost:7233")
    plugin = MCPProxyPlugin(
        options=Options(
            upstreams=[
                Upstream(
                    service="github",
                    client_factory=lambda: Client("https://api.example.com/mcp"),
                    activity_options_overrides={
                        "create_issue": {
                            "start_to_close_timeout": timedelta(minutes=5)
                        },
                    },
                )
            ]
        )
    )
    worker = Worker(client, task_queue="mcp-proxy", plugins=[plugin])
    await worker.run()
```

Create a Nexus endpoint that targets the Worker task queue. Callers then use the service like any
Nexus service:

| Nexus operation | Upstream |
| --- | --- |
| `list_tools` | Calls `tools/list`. Returns the last successful list if the upstream is down. |
| Tool name | Starts an Activity that calls `tools/call`. Returns the MCP tool result. |

If the upstream tool returns a result with `isError` set, the caller gets that result and the
Activity completes. A failed upstream call fails the Activity. See [Failures](#failures).

### Upstream options

| Field | Purpose |
| --- | --- |
| `service` | Nexus service name. Required. |
| `client_factory` | Returns an `mcp.Client`. It sets the transport, URL, and credentials. Required. |
| `activity_options_overrides` | `start_activity` options per tool name, such as timeouts, retry policy, and heartbeat timeout. |
| `classify_error` | Overrides the failure classification. |

The proxy calls `client_factory` once for each discovery and each Activity attempt. The credentials
stay in the Worker, because the Activity input holds only the tool name and arguments.

### Activity defaults

- Start-to-close timeout: 1 minute.
- Retry policy: the Temporal default. A tool annotated as destructive, not read-only, and not
  idempotent gets one attempt.
- Heartbeats: off. Set `heartbeat_timeout` to turn them on. Heartbeats deliver cancellation to the
  running call.

### Failures

A failed upstream call fails the Activity with one of these types. The failure message holds only
the type, because upstream messages can hold private data.

| Type | Cause | Retryable |
| --- | --- | --- |
| `UpstreamConnectionError` | Network error, closed connection, or timeout. | Yes |
| `UpstreamAuthenticationError` | HTTP 401 or 403, or permission denied. | No |
| `UpstreamProtocolError` | Invalid MCP or JSON-RPC response. | No |
| `ProxyConfigurationError` | Missing command or file, or an invalid `classify_error` result. | No |
| `UpstreamError` | Any other error, including other HTTP error statuses. | No |

## Development

See [CONTRIBUTING.md](../.github/CONTRIBUTING.md).
