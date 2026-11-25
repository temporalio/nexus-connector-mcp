# Go module

Go module for the Nexus MCP connector: `github.com/temporalio/nexus-connector-mcp/go`.

> [!NOTE]
> This project is currently experimental. APIs are subject to change.

## Features

| Feature | Package | Entry point |
| --- | --- | --- |
| Inbound connector: serves Nexus operations as MCP tools. | `inbound` | `NewServer` returns an `*mcp.Server`. |
| Outbound proxy: runs upstream MCP tool calls as Activities behind a Nexus service. | `outbound` | `NewPlugin` returns a Worker plugin. |
| Authoring: marks Nexus operations as MCP tools. | `nexusmcp` | `Tool` marks an operation. `Register` registers the marked operations and `list_tools`. |

The Workflow client is in the [Python](../python) and [TypeScript](../typescript) packages.

## Development

See [CONTRIBUTING.md](../.github/CONTRIBUTING.md).
