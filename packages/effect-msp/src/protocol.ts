/**
 * effect-msp/protocol — raw JSON-RPC 2.0-over-NDJSON-stdio framing for MSP.
 *
 * MSP v1's client-facing surface only ever receives *notifications* from the
 * host (turn/item/approval/userInput/session lifecycle events — the full
 * `MspNotification` method vocabulary) and *responses* to requests the
 * client itself sent. The host never sends the client an id-bearing request
 * in this surface, so unlike `effect-codex-app-server` (which patches ACP's
 * bidirectional shape) this protocol layer is one-directional: client sends
 * requests, host sends responses and notifications.
 *
 * Adapted from `effect-codex-app-server/protocol.ts`, the closest existing
 * precedent for a hand-rolled JSON-RPC-over-stdio transport in this repo.
 *
 * @module effect-msp/protocol
 */
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import type * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";

import * as MspError from "./errors.ts";
import { JsonRpcResponseEnvelope } from "./_internal/shared.ts";

const isJsonRpcResponseEnvelope = Schema.is(JsonRpcResponseEnvelope);
const isMspError = Schema.is(MspError.MspError);
const DEFAULT_REQUEST_TIMEOUT_MS = 60_000;
export interface MspProtocolLogEvent {
  readonly direction: "incoming" | "outgoing";
  readonly stage: "raw" | "decoded" | "decode_failed";
  readonly payload: unknown;
}

export interface MspIncomingNotification {
  readonly method: string;
  readonly params?: unknown;
}

export interface MspPatchedProtocolOptions {
  readonly stdio: Stdio.Stdio;
  readonly terminationError?: Effect.Effect<MspError.MspError>;
  readonly logIncoming?: boolean;
  readonly logOutgoing?: boolean;
  readonly logger?: (event: MspProtocolLogEvent) => Effect.Effect<void, never>;
  readonly onTermination?: (error: MspError.MspError) => Effect.Effect<void, never>;
  /** Default deadline applied to every `request()` call. MSP requests await
   * an unbounded `Deferred` with no protocol-level deadline, so a live but
   * nonresponsive host would otherwise hang initialize/session/turn/approval
   * calls indefinitely. Defaults to 60s; override per connection if a
   * caller needs a different bound. */
  readonly requestTimeoutMs?: number;
}

export interface MspPatchedProtocol {
  readonly incomingNotifications: Stream.Stream<MspIncomingNotification, MspError.MspError>;
  readonly request: (
    method: string,
    payload?: unknown,
  ) => Effect.Effect<unknown, MspError.MspError>;
}

interface MspPendingRequest {
  readonly deferred: Deferred.Deferred<unknown, MspError.MspError>;
  readonly method: string;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isIncomingNotification(value: unknown): value is MspIncomingNotification {
  return isObject(value) && typeof value.method === "string" && !("id" in value);
}

function isIncomingResponse(value: unknown): value is typeof JsonRpcResponseEnvelope.Type {
  return isJsonRpcResponseEnvelope(value);
}

const encodeJsonString = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const decodeJsonString = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

const encodeWireMessage = (
  message: Record<string, unknown>,
): Effect.Effect<string, MspError.MspProtocolParseError> =>
  encodeJsonString(message).pipe(
    Effect.map((encoded) => `${encoded}\n`),
    Effect.mapError((cause) => {
      const method = typeof message.method === "string" ? message.method : undefined;
      const requestId =
        typeof message.id === "string" || typeof message.id === "number"
          ? String(message.id)
          : undefined;
      return MspError.MspProtocolParseError.fromSchemaError("encode-wire-message", cause, {
        ...(method === undefined ? {} : { method }),
        ...(requestId === undefined ? {} : { requestId }),
      });
    }),
  );

const decodeWireMessage = (line: string): Effect.Effect<unknown, MspError.MspProtocolParseError> =>
  decodeJsonString(line).pipe(
    Effect.mapError((cause) =>
      MspError.MspProtocolParseError.fromSchemaError("decode-wire-message", cause),
    ),
  );

const normalizeError = (
  error: unknown,
  operation: MspError.MspTransportOperation,
): MspError.MspError =>
  isMspError(error) ? error : new MspError.MspTransportError({ operation, cause: error });

export const makeMspPatchedProtocol = Effect.fn("makeMspPatchedProtocol")(function* (
  options: MspPatchedProtocolOptions,
): Effect.fn.Return<MspPatchedProtocol, never, Scope.Scope> {
  const outgoing = yield* Queue.unbounded<string, Cause.Done<void>>();
  // Unbounded and lossless: MuseAdapter derives item/approval state from these
  // notifications, so a sliding/dropping buffer could silently lose content
  // a consumer never gets a chance to recover.
  const incomingNotifications = yield* Queue.unbounded<
    MspIncomingNotification,
    MspError.MspError
  >();
  const pending = yield* Ref.make(new Map<string, MspPendingRequest>());
  const nextRequestId = yield* Ref.make(1);
  const remainder: Array<string> = [];
  const terminationHandled = yield* Ref.make(false);
  const terminationFailure = yield* Ref.make(Option.none<MspError.MspError>());
  const terminationSignal = yield* Deferred.make<void>();

  const logProtocol = (event: MspProtocolLogEvent) => {
    if (event.direction === "incoming" && !options.logIncoming) return Effect.void;
    if (event.direction === "outgoing" && !options.logOutgoing) return Effect.void;
    return (
      options.logger?.(event) ??
      Effect.logDebug("Muse MSP protocol event").pipe(Effect.annotateLogs({ event }))
    );
  };

  const failAllPending = (error: MspError.MspError) =>
    Ref.get(pending).pipe(
      Effect.flatMap((current) =>
        Effect.forEach([...current.values()], ({ deferred }) => Deferred.fail(deferred, error), {
          discard: true,
        }),
      ),
      Effect.andThen(Ref.set(pending, new Map())),
    );

  const handleTermination = (classify: () => Effect.Effect<MspError.MspError>) =>
    Ref.modify(terminationHandled, (handled) => {
      if (handled) return [Effect.void, true] as const;
      return [
        Effect.gen(function* () {
          const error = yield* classify();
          yield* Ref.set(terminationFailure, Option.some(error));
          yield* failAllPending(error);
          yield* Queue.end(outgoing);
          yield* Queue.fail(incomingNotifications, error);
          yield* Deferred.succeed(terminationSignal, undefined);
          if (options.onTermination) yield* options.onTermination(error);
        }),
        true,
      ] as const;
    }).pipe(Effect.flatten);

  const offerOutgoing = (message: Record<string, unknown>) =>
    Effect.gen(function* () {
      const failure = yield* Ref.get(terminationFailure);
      if (Option.isSome(failure)) return yield* failure.value;

      yield* logProtocol({ direction: "outgoing", stage: "decoded", payload: message });
      const encoded = yield* encodeWireMessage(message);
      yield* logProtocol({ direction: "outgoing", stage: "raw", payload: encoded });
      const accepted = yield* Queue.offer(outgoing, encoded);
      if (!accepted) {
        const closed = yield* Ref.get(terminationFailure);
        return yield* Option.getOrElse(closed, () => new MspError.MspInputStreamEndedError({}));
      }
    });

  const removePending = (requestId: string) =>
    Ref.update(pending, (current) => {
      if (!current.has(requestId)) return current;
      const next = new Map(current);
      next.delete(requestId);
      return next;
    });

  const resolvePending = (
    requestId: string,
    handler: (pendingRequest: MspPendingRequest) => Effect.Effect<void>,
  ) =>
    Ref.modify(pending, (current) => {
      const pendingRequest = current.get(requestId);
      if (!pendingRequest) return [Effect.void, current] as const;
      const next = new Map(current);
      next.delete(requestId);
      return [handler(pendingRequest), next] as const;
    }).pipe(Effect.flatten);

  const handleResponse = (response: typeof JsonRpcResponseEnvelope.Type) => {
    const requestId = String(response.id);
    const protocolError = response.error;
    if (protocolError !== undefined) {
      return resolvePending(requestId, ({ deferred, method }) =>
        Deferred.fail(
          deferred,
          MspError.MspRequestError.fromProtocolError(protocolError, method, requestId),
        ),
      );
    }
    return resolvePending(requestId, ({ deferred }) => Deferred.succeed(deferred, response.result));
  };

  const handleNotification = (notification: MspIncomingNotification) =>
    Queue.offer(incomingNotifications, notification).pipe(Effect.asVoid);

  const routeMessage = Effect.fnUntraced(function* (message: unknown) {
    if (Option.isSome(yield* Ref.get(terminationFailure))) return;
    if (isIncomingResponse(message)) return yield* handleResponse(message);
    if (isIncomingNotification(message)) return yield* handleNotification(message);
    return yield* MspError.MspProtocolParseError.fromUnroutableMessage(message);
  });

  const handleLine = (line: string): Effect.Effect<void, MspError.MspError> => {
    if (line.trim().length === 0) return Effect.void;
    return logProtocol({ direction: "incoming", stage: "raw", payload: line }).pipe(
      Effect.flatMap(() => decodeWireMessage(line)),
      Effect.tap((decoded) =>
        logProtocol({ direction: "incoming", stage: "decoded", payload: decoded }),
      ),
      Effect.tapErrorTag("MspProtocolParseError", (error) =>
        logProtocol({
          direction: "incoming",
          stage: "decode_failed",
          payload: { operation: error.operation, method: error.method, requestId: error.requestId },
        }),
      ),
      Effect.flatMap(routeMessage),
    );
  };

  yield* options.stdio.stdin.pipe(
    Stream.interruptWhen(Deferred.await(terminationSignal)),
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      Effect.sync(() => {
        const lines: Array<string> = [];
        let start = 0;
        for (
          let newline = chunk.indexOf("\n");
          newline !== -1;
          newline = chunk.indexOf("\n", start)
        ) {
          remainder.push(chunk.slice(start, newline));
          lines.push(remainder.join("").replace(/\r$/, ""));
          remainder.length = 0;
          start = newline + 1;
        }
        if (start < chunk.length) remainder.push(chunk.slice(start));
        return lines;
      }).pipe(Effect.flatMap((lines) => Effect.forEach(lines, handleLine, { discard: true }))),
    ),
    // `matchCauseEffect`, not `matchEffect`: a defect (an incoming logger
    // or the chunk-splitting sync block throwing, not just a typed E
    // failure) must still run the termination path, or pending RPCs and the
    // notification consumer stay blocked forever with this reader dead.
    Effect.matchCauseEffect({
      onFailure: (cause) =>
        handleTermination(() =>
          Effect.succeed(normalizeError(Cause.squash(cause), "read-input-stream")),
        ),
      onSuccess: () =>
        Effect.sync(() => {
          const line = remainder.join("");
          remainder.length = 0;
          return line;
        }).pipe(
          Effect.flatMap(handleLine),
          Effect.matchCauseEffect({
            onFailure: (cause) =>
              handleTermination(() =>
                Effect.succeed(normalizeError(Cause.squash(cause), "read-input-stream")),
              ),
            onSuccess: () =>
              handleTermination(
                () =>
                  options.terminationError ??
                  Effect.succeed(new MspError.MspInputStreamEndedError({})),
              ),
          }),
        ),
    }),
    Effect.forkScoped,
  );

  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(options.stdio.stdout()),
    Effect.catchCause((cause) =>
      handleTermination(() =>
        Effect.succeed(normalizeError(Cause.squash(cause), "write-output-stream")),
      ),
    ),
    Effect.forkScoped,
  );

  const requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;

  const request = (method: string, payload?: unknown) =>
    Effect.gen(function* () {
      const requestId = yield* Ref.modify(
        nextRequestId,
        (current) => [current, current + 1] as const,
      );
      const deferred = yield* Deferred.make<unknown, MspError.MspError>();
      // A bracket, not two separate cleanup hooks: an interrupt during
      // `offerOutgoing` itself (not just during the later await) must still
      // remove this pending entry, or it's retained for the lifetime of a
      // persistent connection with nothing left that can ever resolve it.
      return yield* Effect.acquireUseRelease(
        Ref.update(pending, (current) =>
          new Map(current).set(String(requestId), { deferred, method }),
        ),
        () =>
          offerOutgoing({
            jsonrpc: "2.0",
            id: requestId,
            method,
            ...(payload !== undefined ? { params: payload } : {}),
          }).pipe(Effect.andThen(Deferred.await(deferred))),
        () => removePending(String(requestId)),
      ).pipe(
        Effect.timeoutOrElse({
          duration: `${requestTimeoutMs} millis`,
          orElse: () =>
            Effect.fail(
              new MspError.MspTransportError({
                operation: "await-response",
                cause: new Error(
                  `Muse MSP request '${method}' timed out after ${requestTimeoutMs}ms.`,
                ),
              }),
            ),
        }),
      );
    });

  return {
    incomingNotifications: Stream.fromQueue(incomingNotifications),
    request,
  } satisfies MspPatchedProtocol;
});
