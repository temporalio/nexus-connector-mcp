/**
 * Expose Temporal Nexus operations as MCP tools.
 *
 * The Workflow client is in `@temporalio/nexus-mcp/workflow`.
 *
 * Build the Nexus service handler from the service definition as usual, with each
 * tool handler wrapped in {@link tool}. Then pass the service handler to
 * {@link serviceHandler}, and register the result on the Worker:
 *
 * ```ts
 * const handler = serviceHandler(
 *   nexus.serviceHandler(greetService, {
 *     greet: tool({ handler: greet, description: 'Greet someone.', inputSchema: GreetInput }),
 *   }),
 * );
 * ```
 *
 * @module
 */

import type { ListToolsResult, Tool, ToolAnnotations } from '@modelcontextprotocol/sdk/types.js';
import * as nexus from 'nexus-rpc';
import type {
  OperationDefinition,
  OperationHandler,
  OperationMap,
  ServiceDefinition,
  ServiceHandler,
  SyncOperationHandler,
} from 'nexus-rpc';
import * as z from 'zod';

const LIST_TOOLS_OPERATION = 'list_tools';
// Operation names with this prefix are reserved for Temporal.
const RESERVED_PREFIX = '__temporal_';

type JSONObjectSchema = Tool['inputSchema'];

/**
 * Add the `list_tools` operation to a Nexus service definition.
 *
 * `list_tools` takes no input and returns `{ tools: [...] }`, an MCP `tools/list`
 * result. Use the returned definition for typed callers of `list_tools`.
 * {@link serviceHandler} adds the operation to the handler definition, so the
 * handler side does not need it.
 *
 * @throws TypeError if the definition already has a `list_tools` operation.
 */
export function service<Ops extends OperationMap>(definition: ServiceDefinition<Ops>): ServiceDefinition<Ops> {
  if (hasListTools(definition)) {
    throw new TypeError(
      `@temporalio/nexus-mcp: service "${definition.name}" must not declare ${LIST_TOOLS_OPERATION}; service() adds it`,
    );
  }
  return nexus.service(definition.name, {
    ...definition.operations,
    [LIST_TOOLS_OPERATION]: nexus.operation<null, ListToolsResult>({ name: LIST_TOOLS_OPERATION }),
  }) as unknown as ServiceDefinition<Ops>;
}

/**
 * Implement the `list_tools` operation on a Nexus service handler.
 *
 * Build `handler` with `nexus.serviceHandler` from the service definition without
 * `list_tools`. The result has the same operations plus `list_tools`. `list_tools`
 * lists the operations whose handlers {@link tool} marked.
 *
 * @throws TypeError if the handler definition already has `list_tools`, or a tool
 *   name is empty or reserved.
 */
export function serviceHandler<Ops extends OperationMap>(handler: ServiceHandler<Ops>): ServiceHandler<Ops> {
  const { definition } = handler;
  if (hasListTools(definition)) {
    throw new TypeError(
      `@temporalio/nexus-mcp: build the service handler from the definition without ${LIST_TOOLS_OPERATION}`,
    );
  }
  const tools: Tool[] = [];
  for (const [key, operation] of Object.entries(definition.operations) as [string, OperationDefinition<any, any>][]) {
    const marked = markedTools.get(handler.handlers[key] as object);
    if (marked === undefined) {
      continue;
    }
    const name = operation.name || key;
    if (!name.trim() || name.startsWith(RESERVED_PREFIX)) {
      throw new TypeError(`@temporalio/nexus-mcp: tool name "${name}" must be non-empty and not reserved`);
    }
    tools.push({ ...marked, name });
  }
  // The tool list does not change after the handler is built.
  const manifest: ListToolsResult = { tools };
  const listTools: SyncOperationHandler<null, ListToolsResult> = async () => manifest;
  return nexus.serviceHandler(service(definition), {
    ...handler.handlers,
    [LIST_TOOLS_OPERATION]: listTools,
  }) as unknown as ServiceHandler<Ops>;
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

/** The tool definition of each marked handler, without the name. */
const markedTools = new WeakMap<object, Omit<Tool, 'name'>>();

/**
 * Mark a Nexus operation as an MCP tool.
 *
 * The JSON schemas come from the Zod schemas. Only an object output schema is
 * published, because other outputs become text content.
 *
 * @returns The handler, to register on the Nexus service handler.
 * @throws TypeError if the input schema does not describe an object, or a schema
 *   has no JSON Schema form.
 */
export function tool<I, O>(options: ToolOptions<I, O>): OperationHandler<I, O> | SyncOperationHandler<I, O> {
  const inputSchema = z.toJSONSchema(options.inputSchema, { io: 'input' });
  if (inputSchema.type !== 'object') {
    throw new TypeError('@temporalio/nexus-mcp: tool inputSchema must describe an object');
  }
  const outputSchema =
    options.outputSchema === undefined ? undefined : z.toJSONSchema(options.outputSchema, { io: 'output' });
  const definition: Omit<Tool, 'name'> = {
    description: options.description,
    inputSchema: inputSchema as JSONObjectSchema,
    ...(outputSchema?.type === 'object' ? { outputSchema: outputSchema as JSONObjectSchema } : {}),
    ...(options.title === undefined ? {} : { title: options.title }),
    ...(options.annotations === undefined ? {} : { annotations: options.annotations }),
    ...(options.meta === undefined ? {} : { _meta: options.meta }),
  };
  markedTools.set(options.handler, definition);
  return options.handler;
}

function hasListTools(definition: ServiceDefinition): boolean {
  return Object.entries(definition.operations).some(
    ([key, operation]) => key === LIST_TOOLS_OPERATION || operation.name === LIST_TOOLS_OPERATION,
  );
}
