import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';
import test from 'ava';
import { ApplicationFailure, CancelledFailure } from '@temporalio/workflow';
import { NexusMCPClient } from '../workflow.js';

const TOOLS = {
  tools: [
    { name: 'lookup', inputSchema: { type: 'object' } },
    { name: 'slow', inputSchema: { type: 'object' } },
  ],
};

interface Fake {
  results: Record<string, unknown>;
  calls: [string, unknown][];
}

/** Return a client whose Nexus operations return or throw the entry in results. */
function fakeClient(results: Record<string, unknown> = {}): { client: NexusMCPClient; fake: Fake } {
  const fake: Fake = { results: { list_tools: TOOLS, ...results }, calls: [] };
  const client = new NexusMCPClient({ service: 'weather', endpoint: 'weather-endpoint' });
  // execute is the only method that calls the Workflow SDK.
  (client as unknown as { execute: (operation: string, input: unknown) => Promise<unknown> }).execute = async (
    operation,
    input,
  ) => {
    fake.calls.push([operation, input]);
    const result = fake.results[operation];
    if (result instanceof Error) {
      throw result;
    }
    return result;
  };
  return { client, fake };
}

test('constructor requires service and endpoint', (t) => {
  t.throws(() => new NexusMCPClient({ service: '', endpoint: 'endpoint' }), { message: /required/ });
  t.throws(() => new NexusMCPClient({ service: 'service', endpoint: ' ' }), { message: /required/ });
});

test('listTools returns the tool definitions', async (t) => {
  const { client, fake } = fakeClient();
  const tools = await client.listTools();
  t.deepEqual(
    tools.map((tool) => tool.name),
    ['lookup', 'slow'],
  );
  t.deepEqual(fake.calls, [['list_tools', null]]);
});

test('listTools rejects duplicate tool names', async (t) => {
  const { client, fake } = fakeClient();
  fake.results['list_tools'] = { tools: [TOOLS.tools[0], TOOLS.tools[0]] };
  await t.throwsAsync(client.listTools(), { message: /more than once/ });
});

test('callTool calls the operation of the same name', async (t) => {
  const { client, fake } = fakeClient({ lookup: { answer: 42 } });
  const result = await client.callTool('lookup', { q: 'x' });
  t.deepEqual(result.structuredContent, { answer: 42 });
  t.falsy(result.isError);
  t.deepEqual(result.content, [{ type: 'text', text: '{\n  "answer": 42\n}' }]);

  await client.callTool('lookup');
  t.deepEqual(fake.calls, [
    ['lookup', { q: 'x' }],
    ['lookup', {}],
  ]);
});

test('empty and list_tools names throw InvalidParams', async (t) => {
  const { client, fake } = fakeClient();
  for (const name of ['', 'list_tools']) {
    const err = await t.throwsAsync(client.callTool(name), { instanceOf: McpError });
    t.is(err?.code, ErrorCode.InvalidParams);
  }
  t.deepEqual(fake.calls, []);
});

test('operation failure becomes a tool error with the cause message', async (t) => {
  const handlerFailure = ApplicationFailure.create({
    message: 'handler error',
    cause: ApplicationFailure.create({ message: 'tool failed' }),
  });
  const failure = new Error('nexus operation failed', { cause: handlerFailure });
  const { client } = fakeClient({ lookup: failure });
  const result = await client.callTool('lookup');
  t.true(result.isError);
  t.deepEqual(result.content, [{ type: 'text', text: 'tool failed' }]);
});

test('cancellation is rethrown', async (t) => {
  const { client } = fakeClient({ slow: new CancelledFailure('cancelled') });
  await t.throwsAsync(client.callTool('slow'), { instanceOf: CancelledFailure });
});

for (const [name, value, text, structured] of [
  ['string', 'hello', 'hello', undefined],
  ['null', null, '', undefined],
  ['array', [1, 'b'], '[1,"b"]', undefined],
] as const) {
  test(`result mapping: ${name}`, async (t) => {
    const { client } = fakeClient({ lookup: value });
    const result = await client.callTool('lookup');
    t.deepEqual(result.content, [{ type: 'text', text }]);
    t.is(result.structuredContent, structured);
  });
}

test('envelope keeps content, _meta, and the error flag', async (t) => {
  const { client } = fakeClient({
    lookup: {
      'io.temporal/mcpCallToolResult': {
        content: [{ type: 'image', data: '', mimeType: 'image/png' }],
        isError: true,
        _meta: { trace: 'id' },
      },
    },
  });
  const result = await client.callTool('lookup');
  t.true(result.isError);
  t.is(result.content[0]?.type, 'image');
  t.deepEqual(result._meta, { trace: 'id' });
});
