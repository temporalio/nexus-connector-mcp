import test from 'ava';
import * as nexus from 'nexus-rpc';
import * as z from 'zod';
import { service, serviceHandler, tool } from '../index.js';
import { NexusMCPClient } from '../workflow.js';

const exampleService = nexus.service('example', {
  greet: nexus.operation<{ name: string }, string>(),
});

test('service is not implemented', (t) => {
  t.throws(() => service(exampleService), { message: /not implemented/ });
});

test('serviceHandler is not implemented', (t) => {
  const handler = nexus.serviceHandler(exampleService, {
    greet: async (_ctx, input) => `Hello, ${input.name}!`,
  });
  t.throws(() => serviceHandler(handler), { message: /not implemented/ });
});

test('tool is not implemented', (t) => {
  t.throws(
    () =>
      tool({
        handler: async (_ctx, input: { name: string }) => `Hello, ${input.name}!`,
        description: 'Greet someone.',
        inputSchema: z.object({ name: z.string() }),
      }),
    { message: /not implemented/ },
  );
});

test('NexusMCPClient is not implemented', (t) => {
  t.throws(() => new NexusMCPClient({ service: 'example', endpoint: 'example-endpoint' }), {
    message: /not implemented/,
  });
});
