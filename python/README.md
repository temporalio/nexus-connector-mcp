# temporalio-nexus-mcp

Python package for the Nexus MCP connector. Import path: `temporalio.nexus_mcp`.

> [!NOTE]
> This project is currently experimental. APIs are subject to change.

## Features

| Feature | Module | Entry points |
| --- | --- | --- |
| Authoring: marks Nexus operations as MCP tools. | `temporalio.nexus_mcp` | `service`, `service_handler`, `tool`. Safe to import in Workflow code. |
| Workflow client: calls Nexus-backed MCP tools from Workflow code. | `temporalio.nexus_mcp.workflow` | `NexusMCPClient`. |
| Outbound proxy: runs upstream MCP tool calls as Activities behind a Nexus service. | `temporalio.nexus_mcp.outbound` | `MCPProxyPlugin`, `Options`, `Upstream`. |

The inbound connector is in the [Go module](../go). AI SDK bindings for `NexusMCPClient` are in
[temporalio/ai-integrations](https://github.com/temporalio/ai-integrations).

## Development

See [CONTRIBUTING.md](../.github/CONTRIBUTING.md).
