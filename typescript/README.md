# @temporalio/nexus-mcp

TypeScript package for the Nexus MCP connector.

> [!NOTE]
> This project is currently experimental. APIs are subject to change.

## Features

| Feature                                                           | Import                           | Entry points                                             |
| ----------------------------------------------------------------- | -------------------------------- | -------------------------------------------------------- |
| Authoring: marks Nexus operations as MCP tools.                   | `@temporalio/nexus-mcp`          | `service`, `serviceHandler`, `tool`. Nexus handler code. |
| Workflow client: calls Nexus-backed MCP tools from Workflow code. | `@temporalio/nexus-mcp/workflow` | `NexusMCPClient`.                                        |

The inbound connector is in the [Go module](../go). The outbound proxy is in the [Go module](../go) and
the [Python package](../python). AI SDK bindings for `NexusMCPClient` are in
[temporalio/ai-integrations](https://github.com/temporalio/ai-integrations).

## Development

See [CONTRIBUTING.md](../.github/CONTRIBUTING.md).
