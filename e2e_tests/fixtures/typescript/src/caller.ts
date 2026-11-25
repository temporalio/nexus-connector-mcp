/**
 * Runs CallerWorkflow once. Reads a CallInput as JSON on stdin and prints a
 * CallOutput as JSON on stdout.
 */

import { randomUUID } from 'node:crypto';
import { text } from 'node:stream/consumers';
import { Client, Connection } from '@temporalio/client';
import { NativeConnection, Worker } from '@temporalio/worker';
import type { CallInput } from './workflows';

async function main(): Promise<void> {
  const input = JSON.parse(await text(process.stdin)) as CallInput;
  const address = process.env.TEMPORAL_ADDRESS!;
  const namespace = process.env.TEMPORAL_NAMESPACE!;
  const taskQueue = `caller-typescript-${randomUUID()}`;
  const worker = await Worker.create({
    connection: await NativeConnection.connect({ address }),
    namespace,
    taskQueue,
    workflowsPath: require.resolve('./workflows'),
  });
  const client = new Client({ connection: await Connection.connect({ address }), namespace });
  const output = await worker.runUntil(
    client.workflow.execute('CallerWorkflow', { workflowId: taskQueue, taskQueue, args: [input] }),
  );
  process.stdout.write(JSON.stringify(output));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
