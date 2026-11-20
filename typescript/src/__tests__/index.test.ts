import type { ListToolsResult } from '@modelcontextprotocol/sdk/types.js';
import test from 'ava';
import * as nexus from 'nexus-rpc';
import * as z from 'zod';
import { service, serviceHandler, tool } from '../index.js';

const GreetInput = z.object({ name: z.string().describe('Who to greet.') });
const GreetOutput = z.object({ message: z.string() });

const greetService = nexus.service('greeter', {
  greet: nexus.operation<z.infer<typeof GreetInput>, z.infer<typeof GreetOutput>>(),
  shout: nexus.operation<z.infer<typeof GreetInput>, string>({ name: 'shout-op' }),
  hidden: nexus.operation<z.infer<typeof GreetInput>, string>(),
});

function buildHandler(): nexus.ServiceHandler<(typeof greetService)['operations']> {
  return serviceHandler(
    nexus.serviceHandler(greetService, {
      greet: tool({
        handler: async (_ctx, input) => ({ message: `Hello, ${input.name}!` }),
        description: 'Greet someone.',
        inputSchema: GreetInput,
        outputSchema: GreetOutput,
        title: 'Greet',
        annotations: { readOnlyHint: true },
        meta: { 'example.com/owner': 'team' },
      }),
      shout: tool({
        handler: async (_ctx, input) => `HELLO, ${input.name.toUpperCase()}!`,
        description: 'Shout a greeting.',
        inputSchema: GreetInput,
        outputSchema: z.string(),
      }),
      hidden: async (_ctx, input) => input.name,
    }),
  );
}

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
  const definition = service(greetService);
  t.is(definition.name, 'greeter');
  t.deepEqual(Object.keys(definition.operations).sort(), ['greet', 'hidden', 'list_tools', 'shout']);
  t.is((definition.operations as Record<string, { name: string }>)['list_tools']?.name, 'list_tools');
  t.throws(() => service(definition), { message: /must not declare list_tools/ });
});

test('list_tools lists only marked operations, by operation name', async (t) => {
  const { tools } = await listTools(buildHandler());
  t.deepEqual(
    tools.map((tool) => tool.name),
    ['greet', 'shout-op'],
  );
});

test('tool definitions carry metadata and object schemas', async (t) => {
  const { tools } = await listTools(buildHandler());
  const greet = tools.find((tool) => tool.name === 'greet');
  t.is(greet?.title, 'Greet');
  t.is(greet?.description, 'Greet someone.');
  t.deepEqual(greet?.annotations, { readOnlyHint: true });
  t.deepEqual(greet?._meta, { 'example.com/owner': 'team' });
  t.is(greet?.inputSchema.type, 'object');
  t.deepEqual(greet?.inputSchema.required, ['name']);
  t.is(greet?.outputSchema?.type, 'object');

  // A string output becomes text content, so it has no output schema.
  const shout = tools.find((tool) => tool.name === 'shout-op');
  t.is(shout?.outputSchema, undefined);
  t.is(shout?.title, undefined);
});

test('the other operations keep working', async (t) => {
  const handler = buildHandler();
  const result = await handler
    .getOperationHandler('greet')
    .start({} as nexus.StartOperationContext, { name: 'Ada' } as never);
  assertSync(result);
  t.deepEqual(result.value, { message: 'Hello, Ada!' });
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
    op: nexus.operation<z.infer<typeof GreetInput>, string>({ name: '__temporal_op' }),
  });
  const handler = nexus.serviceHandler(reserved, {
    op: tool({ handler: async () => '', description: 'Reserved.', inputSchema: GreetInput }),
  });
  t.throws(() => serviceHandler(handler), { message: /not reserved/ });
});
