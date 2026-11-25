# temporalio-nexus-mcp

Python package `temporalio-nexus-mcp`. Import path: `temporalio.nexus_mcp`. It exposes Temporal
Nexus operations as MCP tools and calls them from Workflows.

| Module | Purpose |
| --- | --- |
| `temporalio.nexus_mcp` | Mark Nexus operations as MCP tools. Safe to import in Workflow code. |
| `temporalio.nexus_mcp.workflow` | Call Nexus-backed MCP tools from a Workflow. |

> [!NOTE]
> This project is currently experimental. APIs are subject to change.

## Requirements

- Python 3.11 or later

## Installation

```bash
pip install temporalio-nexus-mcp
```

## Mark operations as tools

Add `@nexus_mcp.service` below `@nexusrpc.service`, and `@nexus_mcp.service_handler` below
`@nexusrpc.handler.service_handler`. They add a `list_tools` operation. Mark each tool with
`@nexus_mcp.tool` above its operation decorator.

```python
from dataclasses import dataclass

import nexusrpc
import nexusrpc.handler
from temporalio import nexus, nexus_mcp


@dataclass
class WeatherInput:
    city: str


@dataclass
class WeatherOutput:
    forecast: str


@nexusrpc.service(name="weather")
@nexus_mcp.service
class WeatherService:
    get_weather: nexusrpc.Operation[WeatherInput, WeatherOutput]


@nexusrpc.handler.service_handler(service=WeatherService)
@nexus_mcp.service_handler
class WeatherHandler:
    @nexus_mcp.tool(description="Get the weather forecast for a city.")
    @nexus.workflow_run_operation
    async def get_weather(
        self, ctx: nexus.WorkflowRunOperationContext, input: WeatherInput
    ) -> nexus.WorkflowHandle[WeatherOutput]:
        return await ctx.start_workflow(WeatherWorkflow.run, input, id=ctx.request_id)
```

Register `WeatherHandler()` on a Worker with `nexus_service_handlers` as usual.

- The tool name is the Nexus operation name.
- The input and output schemas come from the operation types.
- The input type must have an object JSON schema, such as a dataclass or a Pydantic model.
- Sync and async operations work, such as Workflow, Activity, and Update operations.
- `list_tools` lists only the operations marked with `@nexus_mcp.tool`.
- `@nexus_mcp.tool` also takes `title`, `annotations`, and `meta`.

## Serve tools to MCP clients

The inbound connector is in the Go module. It serves the tools of a Nexus service in any
language. See [Serve tools to MCP clients](../go/README.md#serve-tools-to-mcp-clients) in the Go
README.

## Call tools from a Workflow

`NexusMCPClient` lists and calls the tools of a Nexus service from Workflow code.

```python
from temporalio import workflow
from temporalio.nexus_mcp.workflow import NexusMCPClient


@workflow.defn
class AgentWorkflow:
    @workflow.run
    async def run(self, city: str) -> None:
        mcp = NexusMCPClient(service="weather", endpoint="weather-endpoint")
        tools = await mcp.list_tools()
        result = await mcp.call_tool("get_weather", {"city": city})
```

- `list_tools` returns `list[mcp.types.Tool]`.
- `call_tool` returns `mcp.types.CallToolResult`. A failed operation returns a result with
  `is_error` set.
- The Workflow waits for each operation durably. Workflow cancellation cancels the operation.
- Use one client per Workflow execution.

AI SDK bindings for `NexusMCPClient` are in
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

The [OAuth sample](../samples/python/oauth_outbound_connector) connects to an upstream that
requires OAuth.

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

## License

[MIT](LICENSE)
