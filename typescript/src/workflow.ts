/**
 * Call Nexus-backed MCP tools from Workflow code.
 *
 * AI SDK bindings delegate `listTools` and `callTool` to {@link NexusMCPClient}.
 * The bindings live in `temporalio/ai-integrations`.
 *
 * @module
 */

import {
  CallToolResultSchema,
  ErrorCode,
  ListToolsResultSchema,
  McpError,
  type CallToolResult,
  type Tool,
} from '@modelcontextprotocol/sdk/types.js';
import * as nexus from 'nexus-rpc';
import { createNexusServiceClient, isCancellation } from '@temporalio/workflow';

const LIST_TOOLS_OPERATION = 'list_tools';
// A result with only this key is already an MCP tool result. The outbound proxy
// returns upstream results in this envelope.
const RESULT_ENVELOPE_KEY = 'io.temporal/mcpCallToolResult';

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
 *
 * `listTools` runs the `list_tools` operation of the service. `callTool` runs the
 * Nexus operation that has the tool name. The Workflow waits for each operation
 * durably, so a long-running tool needs no polling. Workflow cancellation cancels
 * the operation.
 *
 * Use one client per Workflow execution. Do not share it across Workflows.
 */
export class NexusMCPClient {
  private readonly service: string;
  private readonly endpoint: string;

  constructor(options: NexusMCPClientOptions) {
    if (!options.service.trim() || !options.endpoint.trim()) {
      throw new TypeError('@temporalio/nexus-mcp: service and endpoint are required');
    }
    this.service = options.service;
    this.endpoint = options.endpoint;
  }

  /**
   * List the tools of the Nexus service.
   *
   * @throws Error if the service returns a tool without a name, or the same tool
   *   name more than once.
   */
  async listTools(): Promise<Tool[]> {
    const manifest = ListToolsResultSchema.parse(await this.execute(LIST_TOOLS_OPERATION, null));
    const names = new Set<string>();
    for (const tool of manifest.tools) {
      if (!tool.name) {
        throw new Error(`Service "${this.service}" returned a tool without a name`);
      }
      if (names.has(tool.name)) {
        throw new Error(`Service "${this.service}" returned tool "${tool.name}" more than once`);
      }
      names.add(tool.name);
    }
    return manifest.tools;
  }

  /**
   * Call a tool and wait for its result.
   *
   * The tool name is the operation name. A failed operation, including an
   * operation that the service does not have, returns a result with `isError` set.
   *
   * @throws McpError with code `InvalidParams` if the name is empty or `list_tools`.
   * @throws The cancellation error if the operation or the Workflow is cancelled.
   */
  async callTool(name: string, args?: Record<string, unknown>): Promise<CallToolResult> {
    if (!name || name === LIST_TOOLS_OPERATION) {
      throw new McpError(ErrorCode.InvalidParams, `Unknown tool "${name}"`);
    }
    let value: unknown;
    try {
      value = await this.execute(name, args ?? {});
    } catch (err) {
      if (isCancellation(err)) {
        throw err;
      }
      return errorResult(failureMessage(err));
    }
    return toCallToolResult(value);
  }

  private async execute(operation: string, input: unknown): Promise<unknown> {
    // Tool names come from list_tools at run time, so the service definition has
    // one operation, keyed by the name of the call.
    const service = nexus.service(this.service, { [operation]: nexus.operation<unknown, unknown>() });
    const client = createNexusServiceClient({ service, endpoint: this.endpoint });
    return await client.executeOperation(operation, input);
  }
}

/**
 * Map a Nexus operation result to an MCP tool result. The inbound connector uses
 * the same rules:
 *   - An object with only the `io.temporal/mcpCallToolResult` key becomes the tool
 *     result in it.
 *   - An object becomes structured content plus its JSON text.
 *   - A string becomes text content.
 *   - `null` or `undefined` becomes empty text content.
 *   - Other values become their JSON text.
 */
function toCallToolResult(value: unknown): CallToolResult {
  if (isPlainObject(value)) {
    const keys = Object.keys(value);
    if (keys.length === 1 && keys[0] === RESULT_ENVELOPE_KEY) {
      return CallToolResultSchema.parse(value[RESULT_ENVELOPE_KEY]);
    }
    return { content: [textContent(JSON.stringify(value, null, 2))], structuredContent: value };
  }
  if (typeof value === 'string') {
    return { content: [textContent(value)] };
  }
  if (value === null || value === undefined) {
    return { content: [textContent('')] };
  }
  return { content: [textContent(JSON.stringify(value))] };
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function textContent(text: string): { type: 'text'; text: string } {
  return { type: 'text', text };
}

/**
 * Return the message of the innermost cause of err. A Nexus operation failure wraps
 * the handler failure in one or more causes. The outer messages describe the
 * operation and the failure types, not the tool error.
 */
function failureMessage(err: unknown): string {
  let innermost = err;
  while (innermost instanceof Error && innermost.cause instanceof Error) {
    innermost = innermost.cause;
  }
  return innermost instanceof Error ? innermost.message : String(innermost);
}

function errorResult(message: string): CallToolResult {
  return { content: [textContent(message)], isError: true };
}
