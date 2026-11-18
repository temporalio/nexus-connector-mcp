# Nexus MCP Connector

[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

This repo will host the building block for an MCP client to connect
to MCP servers with Temporal Nexus acting as the durable transport under the hood. Specifically:

- **Inbound:** serves Nexus operations as MCP tools to any MCP client, allowing MCP clients to reach any Nexus service over MCP.
- **Outbound:** acts as a durable proxy to upstream MCP servers, by backing tool calls with Nexus and Standalone Activities.
- **Authoring:** provides building blocks to mark Nexus operations as MCP tools.
- **Workflow client:** call Nexus-backed MCP tools from Workflow code.

> [!NOTE]
> This project is currently experimental. APIs are subject to change.

## Development

See [CONTRIBUTING.md](.github/CONTRIBUTING.md).
