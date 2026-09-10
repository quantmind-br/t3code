import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { assert, it } from "@effect/vitest";

import * as MspClient from "./client.ts";
import { makeInMemoryStdio } from "./_internal/stdio.ts";

const encoder = new TextEncoder();
const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeUnknownJsonString = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const encodeJsonl = (value: unknown) => encoder.encode(`${encodeUnknownJsonString(value)}\n`);

/**
 * Reads one client-written wire message off `output` and decodes it as a
 * `{id, method, params}` request. Manual step-by-step pump (fork the client
 * call, take its request, offer the canned response), matching the pattern
 * `effect-codex-app-server/protocol.test.ts` uses for the same in-memory
 * stdio harness — avoids relying on a second concurrently-scheduled
 * `Stream.runForEach` consumer fiber to drive the exchange.
 */
const takeRequest = (output: Queue.Dequeue<string>) =>
  Queue.take(output).pipe(
    Effect.flatMap(decodeJson),
    Effect.map((message) => message as { id: number; method: string; params: unknown }),
  );

it.effect("effect-msp client handshake and turn round trip", () =>
  Effect.gen(function* () {
    const { stdio, input, output } = yield* makeInMemoryStdio();
    const client = yield* MspClient.makeOverStdio(stdio);

    const notifications: Array<string> = [];
    const collector = yield* client.notifications.pipe(
      Stream.runForEach((notification) =>
        Effect.sync(() => {
          notifications.push(notification.method);
        }),
      ),
      Effect.forkScoped,
    );

    const initializeFiber = yield* Effect.forkScoped(
      client.initialize({ clientInfo: { name: "t3code", version: "0.0.0" } }),
    );
    const initializeRequest = yield* takeRequest(output);
    assert.equal(initializeRequest.method, "initialize");
    yield* Queue.offer(
      input,
      encodeJsonl({
        id: initializeRequest.id,
        result: {
          experimentalApi: false,
          grantedCapabilities: [],
          museHome: "/home/test/.config/muse",
          platformFamily: "unix",
          platformOs: "linux",
          schema: { fingerprint: "sha256:test", version: 1 },
          serverInfo: { name: "muse", version: "1.1.1" },
          userAgent: "muse-test/1.1.1",
        },
      }),
    );
    const initialized = yield* Fiber.join(initializeFiber);
    assert.equal(initialized.serverInfo.name, "muse");
    // The client must follow a successful initialize with the `initialized`
    // notification, or the host rejects every session/* call.
    const initializedNotification = yield* Queue.take(output).pipe(Effect.flatMap(decodeJson));
    assert.deepEqual(initializedNotification, {
      jsonrpc: "2.0",
      method: "initialized",
      params: {},
    });

    const sessionStartFiber = yield* Effect.forkScoped(client.sessionStart({ commandId: "cmd-1" }));
    const sessionStartRequest = yield* takeRequest(output);
    assert.equal(sessionStartRequest.method, "session/start");
    yield* Queue.offer(
      input,
      encodeJsonl({
        id: sessionStartRequest.id,
        result: {
          session: {
            activeTurnId: null,
            createdAt: "2026-01-01T00:00:00Z",
            forkedFrom: null,
            modelId: null,
            path: "/tmp/session.jsonl",
            providerId: null,
            sessionId: "session-1",
            status: "idle",
            turnCount: 0,
            updatedAt: "2026-01-01T00:00:00Z",
            workspaceRoot: "/tmp/work",
          },
          viewCursor: "v:0:0",
        },
      }),
    );
    const started = yield* Fiber.join(sessionStartFiber);
    assert.equal(started.session.sessionId, "session-1");

    const turnStartFiber = yield* Effect.forkScoped(
      client.turnStart({
        commandId: "cmd-2",
        input: [{ type: "text", text: "hi" }],
        sessionId: "session-1",
      }),
    );
    const turnStartRequest = yield* takeRequest(output);
    assert.equal(turnStartRequest.method, "turn/start");
    const commandId = (turnStartRequest.params as { commandId: string }).commandId;
    yield* Queue.offer(
      input,
      encodeJsonl({
        method: "turn/started",
        params: { commandId, sessionId: "session-1", turnId: commandId, viewCursor: "v:0:1" },
      }),
    );
    yield* Queue.offer(
      input,
      encodeJsonl({
        id: turnStartRequest.id,
        result: {
          commandId,
          disposition: "started",
          startedNewTurn: true,
          status: "accepted",
          turnId: commandId,
        },
      }),
    );
    yield* Queue.offer(
      input,
      encodeJsonl({
        method: "turn/completed",
        params: {
          sessionId: "session-1",
          terminal: "completed",
          turnId: commandId,
          viewCursor: "v:0:2",
        },
      }),
    );
    const turn = yield* Fiber.join(turnStartFiber);
    assert.equal(turn.disposition, "started");
    assert.equal(turn.turnId, "cmd-2");

    // Advance the virtual test clock so the notification-consuming fiber gets
    // scheduled to drain `turn/started`/`turn/completed` before we assert on it.
    yield* TestClock.adjust("10 millis");
    yield* Fiber.interrupt(collector);
    assert.deepEqual(notifications, ["turn/started", "turn/completed"]);
  }),
);

it.effect("effect-msp client surfaces a JSON-RPC error as MspRequestError with its kind", () =>
  Effect.gen(function* () {
    const { stdio, input, output } = yield* makeInMemoryStdio();
    const client = yield* MspClient.makeOverStdio(stdio);

    const decideFiber = yield* Effect.forkScoped(
      client.approvalDecide({
        approvalId: "approval-1",
        choiceId: "bogus",
        commandId: "cmd-3",
        requirementId: { approvalId: "approval-1", sourceIndex: 0 },
        sessionId: "session-1",
      }),
    );
    const decideRequest = yield* takeRequest(output);
    assert.equal(decideRequest.method, "approval/decide");
    yield* Queue.offer(
      input,
      encodeJsonl({
        id: decideRequest.id,
        error: { code: -32052, message: "invalid choice", data: { kind: "approvalChoiceInvalid" } },
      }),
    );

    const failure = yield* Fiber.join(decideFiber).pipe(Effect.flip);
    assert.equal(failure._tag, "MspRequestError");
    if (failure._tag === "MspRequestError") {
      assert.equal(failure.kind, "approvalChoiceInvalid");
      assert.equal(failure.code, -32052);
    }
  }),
);

it.effect(
  "effect-msp client does not write a request that timed out while the writer was blocked",
  () =>
    Effect.gen(function* () {
      const writeGate = yield* Deferred.make<void>();
      const { stdio, output } = yield* makeInMemoryStdio({ writeGate });
      const client = yield* MspClient.makeOverStdio(stdio, { requestTimeoutMs: 1_000 });

      // Both requests are enqueued while the pipe is blocked. Only the first is
      // allowed to live long enough to be written; the second gives up first.
      const firstFiber = yield* Effect.forkScoped(
        client.turnStart({
          commandId: "cmd-a",
          input: [{ type: "text", text: "a" }],
          sessionId: "s",
        }),
      );
      const secondFiber = yield* Effect.forkScoped(
        client.turnStart({
          commandId: "cmd-b",
          input: [{ type: "text", text: "b" }],
          sessionId: "s",
        }),
      );
      // Let both requests reach the outgoing queue before expiring anything.
      yield* TestClock.adjust("10 millis");
      // Interrupt the second caller (equivalent to it timing out) while the
      // writer is still blocked on the first line.
      yield* Fiber.interrupt(secondFiber);
      yield* Deferred.succeed(writeGate, undefined);

      const firstRequest = yield* takeRequest(output);
      assert.equal((firstRequest.params as { commandId: string }).commandId, "cmd-a");
      // Let the writer drain whatever else it is going to write.
      yield* TestClock.adjust("10 millis");
      const leftover = yield* Queue.size(output);
      assert.equal(leftover, 0, "the cancelled request must not have been written");

      yield* Fiber.interrupt(firstFiber);
    }),
);

it.effect("effect-msp client session/fork round trip carries the fork's session and history", () =>
  Effect.gen(function* () {
    const { stdio, input, output } = yield* makeInMemoryStdio();
    const client = yield* MspClient.makeOverStdio(stdio);

    const forkFiber = yield* Effect.forkScoped(
      client.sessionFork({
        commandId: "cmd-fork",
        sessionId: "session-1",
        cutPoint: { lastTurnId: "turn-1" },
        excludeItems: false,
      }),
    );
    const forkRequest = yield* takeRequest(output);
    assert.equal(forkRequest.method, "session/fork");
    assert.deepEqual(forkRequest.params, {
      commandId: "cmd-fork",
      sessionId: "session-1",
      cutPoint: { lastTurnId: "turn-1" },
      excludeItems: false,
    });
    yield* Queue.offer(
      input,
      encodeJsonl({
        id: forkRequest.id,
        result: {
          history: {
            items: [
              { itemId: "i-1", kind: "userMessage", turnId: "turn-1" },
              { itemId: "i-2", kind: "agentMessage", turnId: "turn-1", text: "hi" },
            ],
            mode: "inline",
            snapshot: null,
          },
          pendingRequests: [],
          session: {
            activeTurnId: null,
            createdAt: "2026-01-01T00:00:01Z",
            forkedFrom: {
              commandId: "cmd-fork",
              cutCursor: "opaque",
              cutExplicit: true,
              sessionId: "session-1",
            },
            modelId: null,
            path: "/tmp/session-2.jsonl",
            providerId: null,
            sessionId: "session-2",
            status: "idle",
            turnCount: 1,
            updatedAt: "2026-01-01T00:00:01Z",
            workspaceRoot: "/tmp/work",
          },
          viewCursor: "v:1:0",
        },
      }),
    );
    const forked = yield* Fiber.join(forkFiber);
    assert.equal(forked.session.sessionId, "session-2");
    assert.equal(forked.history.items?.length, 2);
    assert.equal(forked.viewCursor, "v:1:0");
  }),
);
