# Nexus MCP Connector

[![Build and test](https://github.com/temporalio/nexus-connector-mcp/actions/workflows/build_and_test.yaml/badge.svg?branch=main)](https://github.com/temporalio/nexus-connector-mcp/actions/workflows/build_and_test.yaml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

This repo will host the building block for an MCP client to connect
to MCP servers with Temporal Nexus acting as the durable transport under the hood. Specifically:

- **Inbound:** serves Nexus operations as MCP tools to any MCP client, allowing MCP clients to reach any Nexus service over MCP.
- **Outbound:** acts as a durable proxy to upstream MCP servers, by backing tool calls with Nexus and Standalone Activities.
- **Authoring:** provides building blocks to mark Nexus operations as MCP tools.
- **Workflow client:** call Nexus-backed MCP tools from Workflow code.

> [!NOTE]
> This project is currently experimental. APIs are subject to change.

## Packages

| Directory | Package | Contents |
| --- | --- | --- |
| [`go/`](go) | `github.com/temporalio/nexus-connector-mcp/go` | Inbound connector, Authoring helpers, Workflow client, upstream MCP proxy Worker plugin. |
| [`python/`](python) | `temporalio-nexus-mcp` (`temporalio.nexus_mcp`) | Authoring helpers, Workflow client, upstream MCP proxy Worker plugin. |
| [`typescript/`](typescript) | `@temporalio/nexus-mcp` | Authoring helpers, Workflow client. |

AI SDK bindings for in-workflow MCP clients to reach a Nexus-backed MCP server will live in [temporalio/ai-integrations](https://github.com/temporalio/ai-integrations).

## Development

See [CONTRIBUTING.md](.github/CONTRIBUTING.md).
