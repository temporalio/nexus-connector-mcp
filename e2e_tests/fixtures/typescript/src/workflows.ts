/** Workflows of the e2e fixtures. */

import { condition, defineUpdate, setHandler, sleep } from '@temporalio/workflow';
import { NexusMCPClient } from '@temporalio/nexus-mcp/workflow';
import type { Input, Output } from './contract';

export const echoUpdate = defineUpdate<Output, [Input]>('echo');

/** Echoes its input. */
export async function EchoWorkflow(input: Input): Promise<Output> {
  return { message: input.message, kind: 'workflow' };
}

/** Runs until it is cancelled. Its update echoes the input. */
export async function KeeperWorkflow(): Promise<void> {
  setHandler(echoUpdate, async (input: Input) => ({ message: input.message, kind: 'update' }));
  await condition(() => false);
}

/** Runs for an hour. */
export async function SlowWorkflow(input: Input): Promise<Output> {
  await sleep('1 hour');
  return { message: input.message, kind: 'workflow' };
}

/** Input of CallerWorkflow. */
export interface CallInput {
  endpoint: string;
  service: string;
  tool: string;
  arguments: Record<string, unknown>;
}

/** Output of CallerWorkflow. */
export interface CallOutput {
  tools: string[];
  result: Awaited<ReturnType<NexusMCPClient['callTool']>>;
}

/** Lists the tools of a Nexus service and calls one, with NexusMCPClient. */
export async function CallerWorkflow(input: CallInput): Promise<CallOutput> {
  const client = new NexusMCPClient({ service: input.service, endpoint: input.endpoint });
  const tools = (await client.listTools()).map((tool) => tool.name).sort();
  const result = await client.callTool(input.tool, input.arguments);
  return { tools, result };
}
