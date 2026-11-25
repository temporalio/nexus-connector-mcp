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

## Development

See [CONTRIBUTING.md](../.github/CONTRIBUTING.md).

## License

[MIT](LICENSE)
