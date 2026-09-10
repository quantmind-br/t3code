/**
 * effect-msp/client — typed MSP client over one `muse serve` host process.
 *
 * One `MspClient` owns exactly one spawned host process (one MSP
 * connection). Callers that want one host per T3 thread create one
 * `MspClient` per thread, matching the OpenCode/Claude/Codex driver
 * precedent of scoping provider process lifetime to the resource that
 * consumes it.
 *
 * @module effect-msp/client
 */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import * as MspError from "./errors.ts";
import * as MspProtocol from "./protocol.ts";
import * as MspSchema from "./schema.ts";
import { makeChildStdio, makeTerminationError } from "./_internal/stdio.ts";

export interface MspClientOptions {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly env?: Record<string, string | undefined>;
  /** Windows `.cmd`/`.bat` launchers need `shell: true` to run at all; see
   * `@t3tools/shared/shell#resolveSpawnCommand`. */
  readonly shell?: boolean | string;
  readonly cwd?: string;
  readonly logIncoming?: boolean;
  readonly logOutgoing?: boolean;
  readonly logger?: (event: MspProtocol.MspProtocolLogEvent) => Effect.Effect<void, never>;
  /** Forwarded to `makeMspPatchedProtocol`; see its doc for the default. */
  readonly requestTimeoutMs?: number;
}

const decodeResult = <A, I>(
  method: string,
  schema: Schema.Codec<A, I>,
  raw: unknown,
): Effect.Effect<A, MspError.MspError> =>
  Schema.decodeUnknownEffect(schema)(raw).pipe(
    Effect.mapError((cause) =>
      MspError.MspProtocolParseError.fromSchemaError("decode-response-payload", cause, { method }),
    ),
  );

/**
 * MSP notification, narrowed by method. The `params` schema for each
 * literal is provided by the caller of `notifications.pipe(Stream.filter(...))`
 * — see `MuseAdapter.ts` for the dispatch table. Kept generic here so this
 * package does not need to schematize every one of MSP's ~30 notification
 * shapes to be useful.
 */
export interface MspNotification {
  readonly method: string;
  readonly params: unknown;
}

export interface MspClient {
  readonly notifications: Stream.Stream<MspNotification, MspError.MspError>;
  readonly initialize: (
    params: MspSchema.InitializeParams,
  ) => Effect.Effect<MspSchema.InitializeResult, MspError.MspError>;
  readonly sessionStart: (
    params: MspSchema.SessionStartParams,
  ) => Effect.Effect<MspSchema.SessionStartResult, MspError.MspError>;
  readonly sessionResume: (
    params: MspSchema.SessionResumeParams,
  ) => Effect.Effect<MspSchema.SessionResumeResult, MspError.MspError>;
  readonly sessionRead: (
    params: MspSchema.SessionReadParams,
  ) => Effect.Effect<MspSchema.SessionReadResult, MspError.MspError>;
  readonly sessionFork: (
    params: MspSchema.SessionForkParams,
  ) => Effect.Effect<MspSchema.SessionForkResult, MspError.MspError>;
  /**
   * Cursor-paged read of a session's view. Pages are ascending and
   * contiguous, so this is what fills a `view/gap` hole.
   */
  readonly viewPage: (
    params: MspSchema.ViewPageParams,
  ) => Effect.Effect<MspSchema.ViewPageResult, MspError.MspError>;
  readonly sessionSetModel: (
    params: MspSchema.SessionSetModelParams,
  ) => Effect.Effect<MspSchema.SessionSetModelResult, MspError.MspError>;
  readonly sessionSetApprovalMode: (
    params: MspSchema.SessionSetApprovalModeParams,
  ) => Effect.Effect<MspSchema.SessionSetApprovalModeResult, MspError.MspError>;
  readonly sessionCompact: (
    params: MspSchema.SessionCompactParams,
  ) => Effect.Effect<MspSchema.SessionCompactResult, MspError.MspError>;
  readonly turnStart: (
    params: MspSchema.TurnStartParams,
  ) => Effect.Effect<MspSchema.TurnStartResult, MspError.MspError>;
  readonly turnInterrupt: (
    params: MspSchema.TurnInterruptParams,
  ) => Effect.Effect<MspSchema.TurnInterruptResult, MspError.MspError>;
  readonly turnSteer: (
    params: MspSchema.TurnSteerParams,
  ) => Effect.Effect<MspSchema.TurnSteerResult, MspError.MspError>;
  readonly approvalDecide: (
    params: MspSchema.ApprovalDecideParams,
  ) => Effect.Effect<MspSchema.ApprovalDecideResult, MspError.MspError>;
  readonly approvalListPending: (
    params: MspSchema.ApprovalListPendingParams,
  ) => Effect.Effect<MspSchema.ApprovalListPendingResult, MspError.MspError>;
  readonly userInputAnswer: (
    params: MspSchema.UserInputAnswerParams,
  ) => Effect.Effect<MspSchema.UserInputAnswerResult, MspError.MspError>;
  readonly userInputCancel: (
    params: MspSchema.UserInputCancelParams,
  ) => Effect.Effect<MspSchema.UserInputCancelResult, MspError.MspError>;
  readonly modelList: (
    params: MspSchema.ModelListParams,
  ) => Effect.Effect<MspSchema.ModelListResult, MspError.MspError>;
}

/**
 * Build a client directly over an already-open `Stdio.Stdio` duplex. Used by
 * tests (in-memory stdio against a scripted fake host) and by `spawn` below
 * (child-process stdio).
 */
export const makeOverStdio = Effect.fn("effect-msp/makeOverStdio")(function* (
  stdio: Stdio.Stdio,
  options: {
    readonly terminationError?: Effect.Effect<MspError.MspError>;
    readonly logIncoming?: boolean;
    readonly logOutgoing?: boolean;
    readonly logger?: (event: MspProtocol.MspProtocolLogEvent) => Effect.Effect<void, never>;
    readonly requestTimeoutMs?: number;
  } = {},
): Effect.fn.Return<MspClient, never, Scope.Scope> {
  const protocol = yield* MspProtocol.makeMspPatchedProtocol({
    stdio,
    ...(options.terminationError ? { terminationError: options.terminationError } : {}),
    ...(options.logIncoming !== undefined ? { logIncoming: options.logIncoming } : {}),
    ...(options.logOutgoing !== undefined ? { logOutgoing: options.logOutgoing } : {}),
    ...(options.logger ? { logger: options.logger } : {}),
    ...(options.requestTimeoutMs !== undefined
      ? { requestTimeoutMs: options.requestTimeoutMs }
      : {}),
  });

  const call = <A, I>(method: string, schema: Schema.Codec<A, I>, payload: unknown) =>
    protocol
      .request(method, payload)
      .pipe(Effect.flatMap((raw) => decodeResult(method, schema, raw)));

  return {
    notifications: Stream.map(protocol.incomingNotifications, (notification) => ({
      method: notification.method,
      params: notification.params,
    })),
    // The host gates every session/* method behind the client's `initialized`
    // notification (not just a successful `initialize` result) — without it,
    // `session/start` fails with `notInitialized`. Sending it here keeps the
    // handshake a single call for every consumer.
    initialize: (params) =>
      call("initialize", MspSchema.InitializeResult, params).pipe(
        Effect.tap(() => protocol.notify("initialized", {})),
      ),
    sessionStart: (params) => call("session/start", MspSchema.SessionStartResult, params),
    sessionResume: (params) => call("session/resume", MspSchema.SessionResumeResult, params),
    sessionRead: (params) => call("session/read", MspSchema.SessionReadResult, params),
    sessionFork: (params) => call("session/fork", MspSchema.SessionForkResult, params),
    viewPage: (params) => call("view/page", MspSchema.ViewPageResult, params),
    sessionSetModel: (params) => call("session/setModel", MspSchema.SessionSetModelResult, params),
    sessionSetApprovalMode: (params) =>
      call("session/setApprovalMode", MspSchema.SessionSetApprovalModeResult, params),
    sessionCompact: (params) => call("session/compact", MspSchema.SessionCompactResult, params),
    turnStart: (params) => call("turn/start", MspSchema.TurnStartResult, params),
    turnInterrupt: (params) => call("turn/interrupt", MspSchema.TurnInterruptResult, params),
    turnSteer: (params) => call("turn/steer", MspSchema.TurnSteerResult, params),
    approvalDecide: (params) => call("approval/decide", MspSchema.ApprovalDecideResult, params),
    approvalListPending: (params) =>
      call("approval/listPending", MspSchema.ApprovalListPendingResult, params),
    userInputAnswer: (params) => call("userInput/answer", MspSchema.UserInputAnswerResult, params),
    userInputCancel: (params) => call("userInput/cancel", MspSchema.UserInputCancelResult, params),
    modelList: (params) => call("model/list", MspSchema.ModelListResult, params),
  } satisfies MspClient;
});

/**
 * Spawn `muse serve` (or an equivalent command/args pair) and build a client
 * over its stdio. The child is scoped: it is killed when the returned
 * scope closes, never by matching its name or pid against a process list.
 */
export const spawn = Effect.fn("effect-msp/spawn")(function* (
  options: MspClientOptions,
): Effect.fn.Return<
  MspClient,
  MspError.MspError,
  Scope.Scope | ChildProcessSpawner.ChildProcessSpawner
> {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const handle = yield* spawner
    .spawn(
      ChildProcess.make(options.command, options.args, {
        cwd: options.cwd,
        env: options.env,
        ...(options.shell !== undefined ? { shell: options.shell } : {}),
      }),
    )
    .pipe(
      Effect.mapError((cause) => new MspError.MspSpawnError({ command: options.command, cause })),
    );

  const stdio = yield* makeChildStdio(handle);
  return yield* makeOverStdio(stdio, {
    terminationError: makeTerminationError(handle),
    ...(options.logIncoming !== undefined ? { logIncoming: options.logIncoming } : {}),
    ...(options.logOutgoing !== undefined ? { logOutgoing: options.logOutgoing } : {}),
    ...(options.logger ? { logger: options.logger } : {}),
    ...(options.requestTimeoutMs !== undefined
      ? { requestTimeoutMs: options.requestTimeoutMs }
      : {}),
  });
});
