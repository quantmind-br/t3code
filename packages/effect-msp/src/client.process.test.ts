/**
 * Runs the client against a REAL child process (a tiny MSP mock host over
 * stdio). The in-memory `Stdio` harness in `client.test.ts` cannot reproduce
 * pipe lifecycle: for a spawned child, `makeChildStdio` wraps `handle.stdin`,
 * whose default `endOnDone: true` ends the writable the moment a stream fed
 * into its sink completes. A writer that ran one finite stream per frame
 * would therefore close the host's stdin after the very first request —
 * `initialize` succeeds, everything after it fails. This test exists to
 * catch exactly that class of regression.
 */
import * as Effect from "effect/Effect";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";

import { assert, it } from "@effect/vitest";

import * as MspClient from "./client.ts";

const mockHostPath = Effect.map(Effect.service(Path.Path), (path) =>
  path.join(import.meta.dirname, "../test/fixtures/msp-mock-host.ts"),
);

it.layer(NodeServices.layer)("effect-msp client over a real child process", (it) => {
  it.effect(
    "writes several consecutive frames (initialize, initialized, session/start) on one stdin",
    () =>
      Effect.gen(function* () {
        const client = yield* MspClient.spawn({
          command: process.execPath,
          args: [yield* mockHostPath],
          env: process.env,
          requestTimeoutMs: 5_000,
        });

        const initialized = yield* client.initialize({
          clientInfo: { name: "t3code_test", version: "0.0.0" },
        });
        assert.equal(initialized.serverInfo.name, "muse-mock");

        // Would fail with `notInitialized` if the `initialized` notification
        // never reached the host, and would hang/fail with a closed stdin if
        // the writer ended the pipe after the first frame.
        const first = yield* client.sessionStart({ commandId: "cmd-1" });
        assert.equal(first.session.sessionId, "session-1");
        const second = yield* client.sessionStart({ commandId: "cmd-2" });
        assert.equal(second.session.sessionId, "session-2");
      }),
  );
});
