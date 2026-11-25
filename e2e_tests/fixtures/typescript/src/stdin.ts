/** Exits when stdin closes, so a fixture stops when the test process dies. */
export function exitOnStdinClose(): void {
  process.stdin.on('end', () => process.exit(0));
  process.stdin.resume();
}
