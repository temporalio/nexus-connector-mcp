/**
 * Expose Temporal Nexus operations as MCP tools.
 *
 * The Workflow client is in `@temporalio/nexus-mcp/workflow`.
 *
 * @module
 */

import type { ToolAnnotations } from '@modelcontextprotocol/sdk/types.js';
import type {
  OperationHandler,
  OperationMap,
  ServiceDefinition,
  ServiceHandler,
  SyncOperationHandler,
} from 'nexus-rpc';
import type * as z from 'zod';

/**
 * Add the `list_tools` operation to a Nexus service definition.
 */
export function service<Ops extends OperationMap>(_definition: ServiceDefinition<Ops>): ServiceDefinition<Ops> {
  throw new Error('@temporalio/nexus-mcp: service is not implemented');
}

/**
 * Implement the `list_tools` operation on a Nexus service handler.
 */
export function serviceHandler<Ops extends OperationMap>(_handler: ServiceHandler<Ops>): ServiceHandler<Ops> {
  throw new Error('@temporalio/nexus-mcp: serviceHandler is not implemented');
}

/**
 * Options for {@link tool}.
 */
export interface ToolOptions<I, O> {
  /** The Nexus operation handler. The tool name is the registered operation name. */
  handler: OperationHandler<I, O> | SyncOperationHandler<I, O>;
  /** Tool description. */
  description: string;
  /** Input schema. It must describe an object. */
  inputSchema: z.ZodType<I>;
  /** Output schema. Omit it for text outputs. */
  outputSchema?: z.ZodType<O>;
  /** Display label for the tool. */
  title?: string;
  /** MCP tool annotations. */
  annotations?: ToolAnnotations;
  /** MCP `_meta` for the tool. */
  meta?: Record<string, unknown>;
}

/**
 * Mark a Nexus operation as an MCP tool.
 *
 * @returns The handler, to register on the Nexus service handler.
 */
export function tool<I, O>(_options: ToolOptions<I, O>): OperationHandler<I, O> | SyncOperationHandler<I, O> {
  throw new Error('@temporalio/nexus-mcp: tool is not implemented');
}
