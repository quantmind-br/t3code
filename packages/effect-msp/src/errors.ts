/**
 * effect-msp/errors — tagged errors for the Muse Session Protocol (MSP)
 * transport and RPC layer.
 *
 * MSP is JSON-RPC 2.0 over NDJSON stdio: one `muse serve` process per
 * connection, requests/responses correlated by id, and server-to-client
 * notifications (no server-initiated requests in the v1 surface used here).
 * The error taxonomy mirrors `effect-codex-app-server/errors.ts` — the
 * closest existing precedent for a hand-rolled JSON-RPC-over-stdio
 * transport in this repo — trimmed to what MSP v1 actually needs.
 *
 * @module effect-msp/errors
 */
import * as Schema from "effect/Schema";

export const MspProtocolParseOperation = Schema.Literals([
  "encode-wire-message",
  "decode-wire-message",
  "route-wire-message",
  "decode-notification-payload",
  "decode-response-payload",
]);
export type MspProtocolParseOperation = typeof MspProtocolParseOperation.Type;

export const MspTransportOperation = Schema.Literals([
  "read-input-stream",
  "write-output-stream",
  "read-process-exit-status",
  "await-response",
]);
export type MspTransportOperation = typeof MspTransportOperation.Type;

export interface MspProtocolErrorShape {
  readonly code: number;
  readonly message: string;
  readonly data?: unknown;
}

/**
 * MSP error `data.kind` values this client branches on. Open on the wire
 * (SS1.6); unrecognized kinds decode fine, this is only for the small set
 * the adapter treats specially (auth gate, busy session, stale approval).
 */
export const MspErrorKind = Schema.Literals([
  "notInitialized",
  "sessionNotFound",
  "sessionInUse",
  "sessionAmbiguous",
  "sessionNotLoaded",
  "sessionStreamMismatch",
  "commandRejected",
  "approvalRequirementStale",
  "approvalAlreadyResolved",
  "approvalChoiceInvalid",
  "approvalNotFound",
  "userInputAlreadySettled",
  "userInputAnswerInvalid",
  "userInputNotFound",
  "viewTruncated",
]);
export type MspErrorKind = typeof MspErrorKind.Type;

export class MspSpawnError extends Schema.TaggedError<MspSpawnError>()("MspSpawnError", {
  command: Schema.optional(Schema.String),
  cause: Schema.Defect(),
}) {
  override get message() {
    return this.command
      ? `Failed to spawn Muse MSP host process for command: ${this.command}`
      : "Failed to spawn Muse MSP host process";
  }
}

export class MspProcessExitedError extends Schema.TaggedError<MspProcessExitedError>()(
  "MspProcessExitedError",
  {
    code: Schema.optional(Schema.Number),
    pid: Schema.optionalKey(Schema.Int),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return this.code === undefined
      ? "Muse MSP host process exited"
      : `Muse MSP host process exited with code ${this.code}`;
  }
}

export class MspProtocolParseError extends Schema.TaggedError<MspProtocolParseError>()(
  "MspProtocolParseError",
  {
    operation: MspProtocolParseOperation,
    method: Schema.optionalKey(Schema.String),
    requestId: Schema.optionalKey(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    const method = this.method === undefined ? "" : ` for method '${this.method}'`;
    return `Muse MSP protocol operation '${this.operation}' failed${method}.`;
  }

  static fromSchemaError(
    operation: MspProtocolParseOperation,
    cause: Schema.SchemaError,
    context: { readonly method?: string; readonly requestId?: string } = {},
  ) {
    return new MspProtocolParseError({ operation, ...context, cause });
  }

  static fromUnroutableMessage(message: unknown) {
    const method =
      typeof message === "object" &&
      message !== null &&
      "method" in message &&
      typeof (message as { method?: unknown }).method === "string"
        ? (message as { method: string }).method
        : undefined;
    return new MspProtocolParseError({
      operation: "route-wire-message",
      ...(method === undefined ? {} : { method }),
    });
  }
}

export class MspTransportError extends Schema.TaggedError<MspTransportError>()(
  "MspTransportError",
  {
    operation: MspTransportOperation,
    pid: Schema.optionalKey(Schema.Int),
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return `Muse MSP transport operation '${this.operation}' failed.`;
  }
}

export class MspInputStreamEndedError extends Schema.TaggedError<MspInputStreamEndedError>()(
  "MspInputStreamEndedError",
  {},
) {
  override get message() {
    return "Muse MSP host input stream ended.";
  }
}

/**
 * One JSON-RPC error response from the host, or a client-side rejection
 * (timeout, host terminated mid-request). `kind` is lifted from
 * `error.data.kind` when present so callers can pattern-match without
 * re-parsing `data`.
 */
export class MspRequestError extends Schema.TaggedError<MspRequestError>()("MspRequestError", {
  code: Schema.Number,
  errorMessage: Schema.String,
  kind: Schema.optional(MspErrorKind),
  data: Schema.optional(Schema.Unknown),
  method: Schema.optionalKey(Schema.String),
  requestId: Schema.optionalKey(Schema.String),
  cause: Schema.optionalKey(Schema.Defect()),
}) {
  override get message() {
    return this.errorMessage;
  }

  static fromProtocolError(error: MspProtocolErrorShape, method: string, requestId: string) {
    const data = error.data as { readonly kind?: unknown } | null | undefined;
    const kind =
      data !== undefined &&
      data !== null &&
      typeof data.kind === "string" &&
      Schema.is(MspErrorKind)(data.kind)
        ? data.kind
        : undefined;
    return new MspRequestError({
      code: error.code,
      errorMessage: error.message,
      ...(kind === undefined ? {} : { kind }),
      ...(error.data !== undefined ? { data: error.data } : {}),
      method,
      requestId,
    });
  }
}

export const MspError = Schema.Union([
  MspRequestError,
  MspSpawnError,
  MspProcessExitedError,
  MspProtocolParseError,
  MspTransportError,
  MspInputStreamEndedError,
]);
export type MspError = typeof MspError.Type;
