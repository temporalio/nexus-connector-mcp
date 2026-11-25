import type { ListToolsResult } from '@modelcontextprotocol/sdk/types.js';
import test from 'ava';
import * as nexus from 'nexus-rpc';
import * as z from 'zod';
import {
  startWorkflow,
  TemporalOperationHandler,
  TemporalOperationResult,
  WorkflowRunOperationHandler,
} from '@temporalio/nexus';
import { service, serviceHandler, tool } from '../index.js';

const LookupInput = z.object({ topic: z.string().describe('The topic to look up.') });
const LookupOutput = z.object({ number: z.number() });
type LookupInput = z.infer<typeof LookupInput>;
type LookupOutput = z.infer<typeof LookupOutput>;

async function lookupWorkflow(_input: LookupInput): Promise<LookupOutput> {
  return { number: 7 };
}

const luckyService = nexus.service('lucky', {
  lookup: nexus.operation<LookupInput, LookupOutput>(),
  hidden: nexus.operation<LookupInput, string>(),
  // Async operations. Each kind backs the operation with a different Temporal
  // primitive.
  run_workflow: nexus.operation<LookupInput, LookupOutput>(),
  start_workflow: nexus.operation<LookupInput, LookupOutput>(),
  start_activity: nexus.operation<LookupInput, LookupOutput>(),
  start_update: nexus.operation<LookupInput, LookupOutput>(),
  // The key differs from the operation name. The tool name is the operation name.
  asyncToken: nexus.operation<LookupInput, string>({ name: 'async_token' }),
  custom_async: nexus.operation<LookupInput, LookupOutput>(),
});

/** The async operation handlers by tool name. */
const asyncHandlers: Record<string, nexus.OperationHandler<LookupInput, unknown>> = {
  run_workflow: new WorkflowRunOperationHandler<LookupInput, LookupOutput>(
    async (ctx, input) => await startWorkflow(ctx, lookupWorkflow, { workflowId: ctx.requestId, args: [input] }),
  ),
  start_workflow: new TemporalOperationHandler<LookupInput, LookupOutput>({
    start: async (ctx, client, input) =>
      await client.startWorkflow(lookupWorkflow, { workflowId: ctx.requestId, args: [input] }),
  }),
  start_activity: new TemporalOperationHandler<LookupInput, LookupOutput>({
    start: async (ctx, client, input) =>
      await client.startActivity<LookupOutput>('lookupActivity', {
        id: ctx.requestId,
        taskQueue: 'lookup',
        args: [input],
        startToCloseTimeout: '1 minute',
      }),
  }),
  start_update: new TemporalOperationHandler<LookupInput, LookupOutput>({
    start: async (_ctx, client, input) =>
      await client
        .getWorkflowHandle<typeof lookupWorkflow>('lookup-workflow')
        .update<LookupOutput, [LookupInput]>('lookup', { args: [input], waitForStage: 'ACCEPTED' }),
  }),
  async_token: new TemporalOperationHandler<LookupInput, string>({
    start: async () => TemporalOperationResult.async<string>('lookup-token'),
  }),
  // A plain Nexus operation handler that starts async.
  custom_async: {
    start: async () => nexus.HandlerStartOperationResult.async('lookup-token'),
    cancel: async () => {},
  } satisfies nexus.OperationHandler<LookupInput, LookupOutput>,
};

function asyncTool<O>(name: string, outputSchema: z.ZodType<O>, description: string) {
  return tool({
    handler: asyncHandlers[name] as nexus.OperationHandler<LookupInput, O>,
    description,
    inputSchema: LookupInput,
    outputSchema,
  });
}

function buildHandler(): nexus.ServiceHandler<(typeof luckyService)['operations']> {
  return serviceHandler(
    nexus.serviceHandler(luckyService, {
      lookup: tool({
        handler: async () => ({ number: 7 }),
        title: 'Look up a number',
        description: 'Look up a number now.',
        inputSchema: LookupInput,
        outputSchema: LookupOutput,
        annotations: { readOnlyHint: true },
        meta: { 'example.com/owner': 'team' },
      }),
      hidden: async (_ctx, input) => input.topic,
      run_workflow: asyncTool('run_workflow', LookupOutput, 'Run a Workflow.'),
      start_workflow: asyncTool('start_workflow', LookupOutput, 'Start a Workflow.'),
      start_activity: asyncTool('start_activity', LookupOutput, 'Start an Activity.'),
      start_update: asyncTool('start_update', LookupOutput, 'Start a Workflow Update.'),
      // A string output becomes text content, so the tool has no output schema.
      asyncToken: asyncTool('async_token', z.string(), 'Return an operation token.'),
      custom_async: asyncTool('custom_async', LookupOutput, 'Start a custom async operation.'),
    }),
  );
}

/**
 * Run the list_tools operation of handler and return its result.
 *
 * It calls the operation handler directly, so the tests need no Worker and no Temporal
 * server. A standalone Nexus operation needs both, so it belongs in an integration test.
 * It also checks that list_tools completes synchronously.
 */
async function listTools(handler: nexus.ServiceHandler<any>): Promise<ListToolsResult> {
  const result = await handler.getOperationHandler('list_tools').start({} as nexus.StartOperationContext, null);
  assertSync(result);
  return result.value as ListToolsResult;
}

function assertSync(
  result: nexus.HandlerStartOperationResult<unknown>,
): asserts result is nexus.HandlerStartOperationResult<unknown> & { value: unknown } {
  if (!('value' in result)) {
    throw new Error('list_tools must complete synchronously');
  }
}

test('service adds list_tools to the definition', (t) => {
  const definition = service(luckyService);
  t.is(definition.name, 'lucky');
  t.true('list_tools' in definition.operations);
  t.is((definition.operations as Record<string, { name: string }>)['list_tools']?.name, 'list_tools');
  t.throws(() => service(definition), { message: /must not declare list_tools/ });
});

test('list_tools lists only marked operations, by operation name', async (t) => {
  const { tools } = await listTools(buildHandler());
  t.deepEqual(tools.map((tool) => tool.name).sort(), [
    'async_token',
    'custom_async',
    'lookup',
    'run_workflow',
    'start_activity',
    'start_update',
    'start_workflow',
  ]);
});

test('tool definitions carry metadata and object schemas', async (t) => {
  const { tools } = await listTools(buildHandler());
  const lookup = tools.find((tool) => tool.name === 'lookup');
  t.is(lookup?.title, 'Look up a number');
  t.is(lookup?.description, 'Look up a number now.');
  t.deepEqual(lookup?.annotations, { readOnlyHint: true });
  t.deepEqual(lookup?._meta, { 'example.com/owner': 'team' });
  t.is(lookup?.inputSchema.type, 'object');
  t.deepEqual(lookup?.inputSchema.required, ['topic']);
  t.is(lookup?.outputSchema?.type, 'object');
});

// Check each kind of async operation. The tool definition comes from the Zod
// schemas, and tool returns the handler as given, so the async start and cancel
// stay with the Temporal SDK.
for (const [name, handler] of Object.entries(asyncHandlers)) {
  test(`async tool: ${name}`, async (t) => {
    const tool = (await listTools(buildHandler())).tools.find((tool) => tool.name === name);
    t.truthy(tool?.description);
    t.is(tool?.inputSchema.type, 'object');
    t.deepEqual(tool?.inputSchema.required, ['topic']);
    if (name === 'async_token') {
      t.is(tool?.outputSchema, undefined);
    } else {
      t.deepEqual(tool?.outputSchema?.required, ['number']);
    }
    t.is<unknown, unknown>(asyncTool(name, LookupOutput, 'Description.'), handler);
  });
}

test('custom async tool starts async', async (t) => {
  const result = await buildHandler()
    .getOperationHandler('custom_async')
    .start({} as nexus.StartOperationContext, { topic: 'cats' } as never);
  t.deepEqual(result, nexus.HandlerStartOperationResult.async('lookup-token'));
});

test('tool rejects a non-object input schema', (t) => {
  t.throws(
    () =>
      tool({
        handler: async (_ctx, input: string) => input,
        description: 'Echo.',
        inputSchema: z.string(),
      }),
    { message: /must describe an object/ },
  );
});

test('serviceHandler rejects a definition that already has list_tools', (t) => {
  const withListTools = nexus.service('with-list-tools', {
    list_tools: nexus.operation<null, string>(),
  });
  const handler = nexus.serviceHandler(withListTools, { list_tools: async () => '' });
  t.throws(() => serviceHandler(handler), { message: /without list_tools/ });
});

test('serviceHandler rejects reserved tool names', (t) => {
  const reserved = nexus.service('reserved', {
    op: nexus.operation<LookupInput, string>({ name: '__temporal_op' }),
  });
  const handler = nexus.serviceHandler(reserved, {
    op: tool({ handler: async () => '', description: 'Reserved.', inputSchema: LookupInput }),
  });
  t.throws(() => serviceHandler(handler), { message: /not reserved/ });
});
