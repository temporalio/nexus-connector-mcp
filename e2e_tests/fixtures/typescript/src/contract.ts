/** Types of the e2e contract service. */

import * as z from 'zod';

export const SERVICE = 'contract';

export const Input = z.object({ message: z.string().describe('the message to echo') });
export const Output = z.object({ message: z.string(), kind: z.string() });
export type Input = z.infer<typeof Input>;
export type Output = z.infer<typeof Output>;

/** Returns the ID of the KeeperWorkflow that the update operation targets. */
export function keeperId(taskQueue: string): string {
  return `contract-keeper-${taskQueue}`;
}
