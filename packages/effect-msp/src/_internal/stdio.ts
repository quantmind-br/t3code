import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as MspError from "../errors.ts";

const encoder = new TextEncoder();

// Drains the child's stderr into the void before returning its Stdio. `stdout: ()
// => Sink.drain` above only discards writes made *to the Stdio service's own
// stderr slot* -- it never touches `handle.stderr`, the child's real stderr
// stream. Left unread, a long-lived `muse serve` process that logs enough
// diagnostics fills that OS pipe and blocks, silently stalling every RPC.
export const makeChildStdio = Effect.fn("makeChildStdio")(function* (
  handle: ChildProcessSpawner.ChildProcessHandle,
) {
  yield* Stream.runDrain(handle.stderr).pipe(Effect.ignore, Effect.forkScoped);
  return Stdio.make({
    args: Effect.succeed([]),
    stdin: handle.stdout,
    stdout: () =>
      Sink.mapInput(handle.stdin, (chunk: string | Uint8Array) =>
        typeof chunk === "string" ? encoder.encode(chunk) : chunk,
      ),
    stderr: () => Sink.drain,
  });
});

export const makeInMemoryStdio = Effect.fn("makeInMemoryStdio")(function* () {
  const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
  const output = yield* Queue.unbounded<string>();
  const decoder = new TextDecoder();

  return {
    stdio: Stdio.make({
      args: Effect.succeed([]),
      stdin: Stream.fromQueue(input),
      stdout: () =>
        Sink.forEach((chunk: string | Uint8Array) =>
          Queue.offer(
            output,
            typeof chunk === "string" ? chunk : decoder.decode(chunk, { stream: true }),
          ),
        ),
      stderr: () => Sink.drain,
    }),
    input,
    output,
  };
});

type ChildProcessTerminationHandle = Pick<
  ChildProcessSpawner.ChildProcessHandle,
  "exitCode" | "pid"
>;

export const makeTerminationError = (
  handle: ChildProcessTerminationHandle,
): Effect.Effect<MspError.MspError> =>
  Effect.match(handle.exitCode, {
    onFailure: (cause) =>
      new MspError.MspTransportError({
        operation: "read-process-exit-status",
        pid: handle.pid,
        cause,
      }),
    onSuccess: (code) => new MspError.MspProcessExitedError({ code, pid: handle.pid }),
  }).pipe(
    // The child may close stdout (EOF) while staying alive a little longer,
    // or exit reporting take a moment to resolve. Once input has ended no
    // response can arrive either way, so bound the wait instead of letting a
    // slow/stuck exit observation stall every pending request indefinitely.
    Effect.timeoutOrElse({
      duration: "2 seconds",
      orElse: () => Effect.succeed(new MspError.MspInputStreamEndedError({})),
    }),
  );
