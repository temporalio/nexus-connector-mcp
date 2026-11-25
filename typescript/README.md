# @temporalio/nexus-mcp

TypeScript package `@temporalio/nexus-mcp`. It exposes Temporal Nexus operations as MCP tools and
calls them from Workflows.

| Import                           | Purpose                                                           |
| -------------------------------- | ----------------------------------------------------------------- |
| `@temporalio/nexus-mcp`          | Mark Nexus operations as MCP tools. Use it in Nexus handler code. |
| `@temporalio/nexus-mcp/workflow` | Call Nexus-backed MCP tools from a Workflow.                      |

> [!NOTE]
> This project is currently experimental. APIs are subject to change.

## Requirements

- Node.js 22.12 or later

## Installation

```bash
npm install @temporalio/nexus-mcp
```

## Mark operations as tools

Wrap each tool handler in `tool`, and pass the service handler to `serviceHandler`. It adds a
`list_tools` operation. The Zod schemas set the tool input and output schemas.

```ts
import * as nexus from 'nexus-rpc';
import * as z from 'zod';
import { WorkflowRunOperationHandler, startWorkflow } from '@temporalio/nexus';
import { serviceHandler, tool } from '@temporalio/nexus-mcp';

const WeatherInput = z.object({ city: z.string().describe('The city name.') });
const WeatherOutput = z.object({ forecast: z.string() });

const weatherService = nexus.service('weather', {
  get_weather: nexus.operation<z.infer<typeof WeatherInput>, z.infer<typeof WeatherOutput>>(),
});

export const weatherHandler = serviceHandler(
  nexus.serviceHandler(weatherService, {
    get_weather: tool({
      handler: new WorkflowRunOperationHandler(async (ctx, input) =>
        startWorkflow(ctx, weatherWorkflow, { workflowId: ctx.requestId, args: [input] }),
      ),
      description: 'Get the weather forecast for a city.',
      inputSchema: WeatherInput,
      outputSchema: WeatherOutput,
    }),
  }),
);
```

Register `weatherHandler` on a Worker with `nexusServices` as usual.

- The tool name is the Nexus operation name.
- The input schema must describe an object. Omit `outputSchema` for text outputs.
- Sync and async operations work, such as Workflow, Activity, and Update operations.
- `list_tools` lists only the operations wrapped in `tool`.
- `tool` also takes `title`, `annotations`, and `meta`.
- `service` adds `list_tools` to the service definition, for typed callers of `list_tools`.

## Serve tools to MCP clients

The inbound connector is in the Go module. It serves the tools of a Nexus service in any
language. See [Serve tools to MCP clients](../go/README.md#serve-tools-to-mcp-clients) in the Go
README.

## Call tools from a Workflow

`NexusMCPClient` lists and calls the tools of a Nexus service from Workflow code.

```ts
import { NexusMCPClient } from '@temporalio/nexus-mcp/workflow';

export async function agentWorkflow(city: string): Promise<void> {
  const mcp = new NexusMCPClient({ service: 'weather', endpoint: 'weather-endpoint' });
  const tools = await mcp.listTools();
  const result = await mcp.callTool('get_weather', { city });
}
```

- `listTools` returns `Tool[]` from `@modelcontextprotocol/sdk`.
- `callTool` returns a `CallToolResult`. A failed operation returns a result with `isError` set.
- The Workflow waits for each operation durably. Workflow cancellation cancels the operation.
- Use one client per Workflow execution.

AI SDK bindings for `NexusMCPClient` are in
[temporalio/ai-integrations](https://github.com/temporalio/ai-integrations).

## Development

See [CONTRIBUTING.md](../.github/CONTRIBUTING.md).

## License

[MIT](LICENSE)
