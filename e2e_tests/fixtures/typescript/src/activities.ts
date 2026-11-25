/** Activities of the e2e fixtures. */

import type { Input, Output } from './contract';

/** Echoes the message. */
export async function echo_activity(input: Input): Promise<Output> {
  return { message: input.message, kind: 'activity' };
}
