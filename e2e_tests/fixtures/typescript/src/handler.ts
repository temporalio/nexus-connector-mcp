/**
 * Worker for the e2e contract service. Prints READY when the Worker runs.
 */

import { parseArgs } from 'node:util';
import * as nexus from 'nexus-rpc';
import { Client, Connection } from '@temporalio/client';
import { TemporalOperationHandler, WorkflowRunOperationHandler, startWorkflow } from '@temporalio/nexus';
import { serviceHandler, tool } from '@temporalio/nexus-mcp';
import { NativeConnection, Worker } from '@temporalio/worker';
import * as activities from './activities';
import { Input, Output, SERVICE, keeperId } from './contract';
import { exitOnStdinClose } from './stdin';

const contractService = nexus.service(SERVICE, {
  sync_echo: nexus.operation<Input, Output>(),
  workflow_run: nexus.operation<Input, Output>(),
  start_workflow: nexus.operation<Input, Output>(),
  start_activity: nexus.operation<Input, Output>(),
  start_update: nexus.operation<Input, Output>(),
  fail: nexus.operation<Input, Output>(),
  slow: nexus.operation<Input, Output>(),
});

function contractHandler(taskQueue: string) {
  const schemas = { inputSchema: Input, outputSchema: Output };
  return serviceHandler(
    nexus.serviceHandler(contractService, {
      sync_echo: tool({
        handler: async (_ctx: nexus.StartOperationContext, input: Input): Promise<Output> => ({
          message: input.message,
          kind: 'sync',
        }),
        description: 'Echo the message in a sync operation.',
        ...schemas,
      }),
      workflow_run: tool({
        handler: new WorkflowRunOperationHandler<Input, Output>(
          async (ctx, input) => await startWorkflow(ctx, 'EchoWorkflow', { workflowId: ctx.requestId, args: [input] }),
        ),
        description: 'Echo the message in a Workflow run.',
        ...schemas,
      }),
      start_workflow: tool({
        handler: new TemporalOperationHandler<Input, Output>({
          start: async (ctx, client, input) =>
            await client.startWorkflow('EchoWorkflow', { workflowId: ctx.requestId, args: [input] }),
        }),
        description: 'Echo the message in a started Workflow.',
        ...schemas,
      }),
      start_activity: tool({
        handler: new TemporalOperationHandler<Input, Output>({
          start: async (ctx, client, input) =>
            await client.startActivity<Output>('echo_activity', {
              id: ctx.requestId,
              taskQueue,
              args: [input],
              startToCloseTimeout: '1 minute',
            }),
        }),
        description: 'Echo the message in an Activity.',
        ...schemas,
      }),
      start_update: tool({
        handler: new TemporalOperationHandler<Input, Output>({
          start: async (_ctx, client, input) =>
            await client
              .getWorkflowHandle(keeperId(taskQueue))
              .update<Output, [Input]>('echo', { args: [input], waitForStage: 'ACCEPTED' }),
        }),
        description: 'Echo the message in a Workflow update.',
        ...schemas,
      }),
      fail: tool({
        handler: async (_ctx: nexus.StartOperationContext, input: Input): Promise<Output> => {
          throw new nexus.OperationError('failed', `failed: ${input.message}`);
        },
        description: 'Fail the operation.',
        ...schemas,
      }),
      slow: tool({
        handler: new WorkflowRunOperationHandler<Input, Output>(
          async (ctx, input) => await startWorkflow(ctx, 'SlowWorkflow', { workflowId: `slow-${input.message}`, args: [input] }),
        ),
        description: 'Run a Workflow for an hour.',
        ...schemas,
      }),
    }),
  );
}

async function main(): Promise<void> {
  exitOnStdinClose();
  const { values } = parseArgs({ options: { 'task-queue': { type: 'string' } } });
  const taskQueue = values['task-queue'];
  if (!taskQueue) {
    throw new Error('--task-queue is required');
  }
  const address = process.env.TEMPORAL_ADDRESS!;
  const namespace = process.env.TEMPORAL_NAMESPACE!;
  const worker = await Worker.create({
    connection: await NativeConnection.connect({ address }),
    namespace,
    taskQueue,
    workflowsPath: require.resolve('./workflows'),
    activities,
    nexusServices: [contractHandler(taskQueue)],
  });
  const client = new Client({ connection: await Connection.connect({ address }), namespace });
  const running = worker.run();
  await client.workflow.start('KeeperWorkflow', {
    workflowId: keeperId(taskQueue),
    taskQueue,
    workflowIdConflictPolicy: 'USE_EXISTING',
  });
  console.log('READY');
  await running;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
