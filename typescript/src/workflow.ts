/**
 * Call Nexus-backed MCP tools from Workflow code.
 *
 * AI SDK bindings delegate `listTools` and `callTool` to {@link NexusMCPClient}.
 * The bindings live in `temporalio/ai-integrations`.
 *
 * @module
 */

import type { CallToolResult, Tool } from '@modelcontextprotocol/sdk/types.js';

/**
 * Options for {@link NexusMCPClient}.
 */
export interface NexusMCPClientOptions {
  /** The Nexus service name. */
  service: string;
  /** The Nexus endpoint name. */
  endpoint: string;
}

/**
 * MCP client that calls tools through Workflow Nexus operations.
 */
export class NexusMCPClient {
  constructor(_options: NexusMCPClientOptions) {
    throw new Error('@temporalio/nexus-mcp: NexusMCPClient is not implemented');
  }

  /** List the tools of the Nexus service. */
  async listTools(): Promise<Tool[]> {
    throw new Error('@temporalio/nexus-mcp: NexusMCPClient.listTools is not implemented');
  }

  /** Call a tool and wait for its result. */
  async callTool(_name: string, _args?: Record<string, unknown>): Promise<CallToolResult> {
    throw new Error('@temporalio/nexus-mcp: NexusMCPClient.callTool is not implemented');
  }
}
