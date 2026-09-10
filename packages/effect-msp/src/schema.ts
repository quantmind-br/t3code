/**
 * effect-msp/schema — MSP v1 wire shapes this client actually uses.
 *
 * Field names and semantics are taken verbatim from the protocol's own
 * generated TypeScript declarations (`muse schema generate-ts`, embedded in
 * the target binary and re-extracted per release) — not guessed. Outgoing
 * params are plain TS interfaces (we construct them, so a compile-time
 * check is enough); incoming results get a minimal `Effect.Schema` decode
 * so a host protocol drift surfaces as a typed decode error instead of a
 * silent `undefined`. Notification payloads are intentionally *not*
 * exhaustively schematized here — MSP v1 ships ~30 notification shapes and
 * this client only reads a handful of fields from each; the adapter decodes
 * what it needs per notification with small local guards.
 *
 * @module effect-msp/schema
 */
import * as Schema from "effect/Schema";

// ---------------------------------------------------------------------------
// Shared open-enum-ish string unions
// ---------------------------------------------------------------------------

export type ApprovalMode = "allowAll" | "promptUnmatched" | "onRequest" | "denyUnmatched";
export type IfBusy = "queue" | "steer" | "replace";
export type ReasoningEffort = "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "ultra";
export type CommandStatus = "accepted" | (string & {});
export type TurnStartDisposition = "started" | "queued" | "steered" | (string & {});
export type TurnTerminal = "completed" | "failed" | "cancelled" | (string & {});
export type ApprovalDecision =
  | "approved"
  | "approvedForSession"
  | "approvedPolicyAmendment"
  | "denied"
  | "deniedPolicyAmendment"
  | "timedOut"
  | "abort"
  | (string & {});
export type UserInputSelectionMode = "single" | "multiple";
export type UserInputOutcome =
  | "answered"
  | "cancelled"
  | "interrupted"
  | "clarified"
  | "timedOut"
  | "aborted"
  | (string & {});
export type HistoryPreference = "auto" | "inline" | "snapshot" | "anchored";
export type HistoryMode = "anchoredSnapshot" | "inline" | "snapshot" | "none" | (string & {});

// ---------------------------------------------------------------------------
// Outgoing params (constructed by this client — TS-checked, not decoded)
// ---------------------------------------------------------------------------

export interface ClientInfo {
  readonly name: string;
  readonly title?: string;
  readonly version: string;
}

export interface ClientCapabilities {
  readonly experimentalApi?: boolean;
  readonly optOutNotificationMethods?: ReadonlyArray<string>;
  readonly requestedCapabilities?: ReadonlyArray<string>;
}

export interface InitializeParams {
  readonly capabilities?: ClientCapabilities;
  readonly clientInfo: ClientInfo;
}

export interface ModelSelectionInput {
  readonly modelId: string;
  readonly profileId?: string | null;
  readonly providerId?: string;
}

export interface SessionStartParams {
  readonly approvalMode?: ApprovalMode | null;
  readonly commandId: string;
  readonly modelId?: string;
  readonly providerId?: string | null;
  readonly sessionId?: string;
  readonly workspaceRoot?: string;
}

export interface SessionResumeParams {
  readonly commandId: string;
  readonly cursor?: string | null;
  readonly excludeItems?: boolean;
  readonly history?: HistoryPreference;
  readonly sessionId: string;
}

export interface SessionReadParams {
  /** Default `true` here (metadata-only); `false` carries the folded item history. */
  readonly excludeItems?: boolean;
  readonly sessionId: string;
}

export interface SessionForkParams {
  readonly commandId: string;
  /** Copy history through this completed turn, inclusive. Omitted = all completed turns. */
  readonly cutPoint?: { readonly lastTurnId: string };
  readonly excludeItems?: boolean;
  readonly sessionId: string;
}

export interface SessionSetModelParams {
  readonly commandId: string;
  readonly model: ModelSelectionInput;
  readonly sessionId: string;
}

export interface SessionSetApprovalModeParams {
  readonly commandId: string;
  readonly mode: ApprovalMode;
  readonly sessionId: string;
}

export interface SessionCompactParams {
  readonly commandId: string;
  readonly sessionId: string;
  readonly turnId?: string;
}

export type TurnInputPart =
  | { readonly type: "text"; readonly text: string }
  | {
      readonly type: "image";
      readonly base64Data: string;
      readonly mediaType: string;
      readonly width?: number;
      readonly height?: number;
    };

export interface TurnStartParams {
  readonly commandId: string;
  readonly displayText?: string;
  readonly ifBusy?: IfBusy;
  readonly input: ReadonlyArray<TurnInputPart>;
  readonly reasoningEffort?: ReasoningEffort;
  readonly sessionId: string;
}

export interface TurnInterruptParams {
  readonly commandId: string;
  readonly retract?: boolean;
  readonly sessionId: string;
  readonly turnId?: string;
}

export interface TurnSteerParams {
  readonly commandId: string;
  readonly expectedTurnId: string;
  readonly input: ReadonlyArray<TurnInputPart>;
  readonly reasoningEffort?: ReasoningEffort;
  readonly sessionId: string;
}

export interface ApprovalRequirementRef {
  readonly approvalId: string;
  readonly sourceIndex: number;
}

export interface ApprovalDecideParams {
  readonly approvalId: string;
  readonly choiceId: string;
  readonly commandId: string;
  readonly feedback?: string | null;
  readonly requirementId: ApprovalRequirementRef;
  readonly sessionId: string;
}

export interface ApprovalListPendingParams {
  readonly sessionId: string;
}

export interface UserInputAnswer {
  readonly freeText?: string;
  readonly note?: string;
  readonly questionId: string;
  readonly selectedLabel?: string;
  readonly selectedLabels?: ReadonlyArray<string>;
}

export interface UserInputAnswerParams {
  readonly answers: ReadonlyArray<UserInputAnswer>;
  readonly commandId: string;
  readonly sessionId: string;
  readonly userInputId: string;
}

export interface UserInputCancelParams {
  readonly commandId: string;
  readonly reason?: string;
  readonly sessionId: string;
  readonly userInputId: string;
}

export interface ModelListParams {
  readonly sessionId?: string;
}

// ---------------------------------------------------------------------------
// Incoming results (decoded defensively via Effect.Schema)
// ---------------------------------------------------------------------------

const UnknownRecord = Schema.Record(Schema.String, Schema.Unknown);

export const SchemaInfo = Schema.Struct({
  fingerprint: Schema.String,
  version: Schema.Number,
});

export const ServerInfo = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
});

export const InitializeResult = Schema.Struct({
  experimentalApi: Schema.Boolean,
  grantedCapabilities: Schema.Array(Schema.String),
  museHome: Schema.String,
  platformFamily: Schema.String,
  platformOs: Schema.String,
  schema: SchemaInfo,
  serverInfo: ServerInfo,
  sessionDurability: Schema.optional(Schema.String),
  userAgent: Schema.String,
});
export type InitializeResult = typeof InitializeResult.Type;

export const Session = Schema.Struct({
  activeTurnId: Schema.NullOr(Schema.String),
  approvalMode: Schema.optional(Schema.Unknown),
  createdAt: Schema.String,
  forkedFrom: Schema.NullOr(Schema.Unknown),
  modelId: Schema.NullOr(Schema.String),
  path: Schema.String,
  providerId: Schema.NullOr(Schema.String),
  sessionId: Schema.String,
  status: Schema.String,
  turnCount: Schema.Number,
  updatedAt: Schema.String,
  workspaceRoot: Schema.NullOr(Schema.String),
});
export type Session = typeof Session.Type;

/**
 * `Item` has ~40 optional kind-specific fields (agentMessage/toolCall/
 * subagent/workflow/…); rather than transcribing all of them, decode it as
 * an open record and let the adapter read the handful of fields it needs
 * defensively (typeof-guarded), matching this package's stated scope.
 */
export type Item = Record<string, unknown> & { readonly itemId: string; readonly kind: string };

export const SessionHistory = Schema.Struct({
  items: Schema.NullOr(Schema.Array(Schema.Unknown)),
  mode: Schema.String,
  noneReason: Schema.optional(Schema.String),
  snapshot: Schema.NullOr(Schema.Unknown),
});
export type SessionHistory = typeof SessionHistory.Type;

export const PendingRequestPointer = Schema.Struct({
  approvalId: Schema.optional(Schema.String),
  kind: Schema.String,
  userInputId: Schema.optional(Schema.String),
  viewCursor: Schema.String,
});
export type PendingRequestPointer = typeof PendingRequestPointer.Type;

export const SessionStartResult = Schema.Struct({
  session: Session,
  viewCursor: Schema.String,
});
export type SessionStartResult = typeof SessionStartResult.Type;

export const SessionResumeResult = Schema.Struct({
  history: SessionHistory,
  pendingRequests: Schema.Array(PendingRequestPointer),
  session: Session,
  viewCursor: Schema.String,
});
export type SessionResumeResult = typeof SessionResumeResult.Type;

/** `session/read` result: same envelope as resume, but a point-in-time read (no subscription). */
export const SessionReadResult = SessionResumeResult;
export type SessionReadResult = typeof SessionReadResult.Type;

/** `session/fork` result: the `session/resume` envelope for the NEW session. */
export const SessionForkResult = SessionResumeResult;
export type SessionForkResult = typeof SessionForkResult.Type;

export const TurnStartResult = Schema.Struct({
  commandId: Schema.String,
  disposition: Schema.String,
  startedNewTurn: Schema.Boolean,
  status: Schema.String,
  turnId: Schema.String,
});
export type TurnStartResult = typeof TurnStartResult.Type;

export const TurnInterruptResult = Schema.Struct({
  commandId: Schema.String,
  status: Schema.String,
  turnId: Schema.String,
});
export type TurnInterruptResult = typeof TurnInterruptResult.Type;

export const TurnSteerResult = Schema.Struct({
  commandId: Schema.String,
  status: Schema.String,
  turnId: Schema.String,
});
export type TurnSteerResult = typeof TurnSteerResult.Type;

export const ApprovalChoice = Schema.Struct({
  acceptsFeedback: Schema.optional(Schema.Boolean),
  choiceId: Schema.String,
  decision: Schema.String,
  label: Schema.String,
  rulePreview: Schema.optional(Schema.String),
  scope: Schema.String,
});
export type ApprovalChoice = typeof ApprovalChoice.Type;

export const ApprovalRequestParams = Schema.Struct({
  approvalId: Schema.String,
  availableChoices: Schema.Array(ApprovalChoice),
  currentRequirementId: Schema.Struct({
    approvalId: Schema.String,
    sourceIndex: Schema.Number,
  }),
  itemId: Schema.String,
  judgeEscalated: Schema.Boolean,
  protectedWrite: Schema.Boolean,
  rawArgs: Schema.String,
  sessionId: Schema.String,
  subject: UnknownRecord,
  taskId: Schema.String,
  toolCallId: Schema.String,
  toolName: Schema.String,
  turnId: Schema.String,
  viewCursor: Schema.String,
});
export type ApprovalRequestParams = typeof ApprovalRequestParams.Type;

export const ApprovalDecideResult = Schema.Struct({
  approvalId: Schema.String,
  commandId: Schema.String,
  status: Schema.String,
  terminal: Schema.Boolean,
});
export type ApprovalDecideResult = typeof ApprovalDecideResult.Type;

export const ApprovalListPendingResult = Schema.Struct({
  approvals: Schema.Array(ApprovalRequestParams),
  userInputs: Schema.Array(UnknownRecord),
});
export type ApprovalListPendingResult = typeof ApprovalListPendingResult.Type;

export const UserInputOption = Schema.Struct({
  description: Schema.optional(Schema.String),
  label: Schema.String,
});
export type UserInputOption = typeof UserInputOption.Type;

export const UserInputQuestion = Schema.Struct({
  header: Schema.String,
  id: Schema.String,
  options: Schema.Array(UserInputOption),
  question: Schema.String,
  selection: Schema.Struct({
    maxSelections: Schema.optional(Schema.Number),
    minSelections: Schema.optional(Schema.Number),
    mode: Schema.String,
  }),
});
export type UserInputQuestion = typeof UserInputQuestion.Type;

export const UserInputRequestParams = Schema.Struct({
  autoResolutionMs: Schema.optional(Schema.Number),
  itemId: Schema.String,
  questions: Schema.Array(UserInputQuestion),
  sessionId: Schema.String,
  toolCallId: Schema.String,
  toolName: Schema.String,
  turnId: Schema.String,
  userInputId: Schema.String,
  viewCursor: Schema.String,
});
export type UserInputRequestParams = typeof UserInputRequestParams.Type;

export const UserInputAnswerResult = Schema.Struct({
  commandId: Schema.String,
  status: Schema.String,
  userInputId: Schema.String,
});
export type UserInputAnswerResult = typeof UserInputAnswerResult.Type;

export const UserInputCancelResult = Schema.Struct({
  commandId: Schema.String,
  status: Schema.String,
  userInputId: Schema.String,
});
export type UserInputCancelResult = typeof UserInputCancelResult.Type;

export const ModelCatalogEntry = Schema.Struct({
  contextLimit: Schema.NullOr(Schema.Number),
  description: Schema.NullOr(Schema.String),
  displayLabel: Schema.String,
  isActive: Schema.Boolean,
  isDefault: Schema.Boolean,
  modelId: Schema.String,
  outputLimit: Schema.NullOr(Schema.Number),
  profileId: Schema.NullOr(Schema.String),
  providerId: Schema.String,
  releaseDate: Schema.NullOr(Schema.String),
});
export type ModelCatalogEntry = typeof ModelCatalogEntry.Type;

export const ModelListResult = Schema.Struct({
  models: Schema.Array(ModelCatalogEntry),
  profileId: Schema.NullOr(Schema.String),
  providerId: Schema.String,
  source: Schema.String,
});
export type ModelListResult = typeof ModelListResult.Type;

export const SessionSetModelResult = Schema.Struct({
  commandId: Schema.String,
  status: Schema.String,
});
export type SessionSetModelResult = typeof SessionSetModelResult.Type;

export const SessionSetApprovalModeResult = Schema.Struct({
  applyOutcome: Schema.String,
  commandId: Schema.String,
  effectiveMode: Schema.Unknown,
  status: Schema.String,
});
export type SessionSetApprovalModeResult = typeof SessionSetApprovalModeResult.Type;

export const SessionCompactResult = Schema.Struct({
  commandId: Schema.String,
  reason: Schema.optional(Schema.String),
  status: Schema.String,
});
export type SessionCompactResult = typeof SessionCompactResult.Type;

// ---------------------------------------------------------------------------
// Notification envelope (method dispatch happens in the adapter)
// ---------------------------------------------------------------------------

export const TurnCompletedParams = Schema.Struct({
  durationMs: Schema.optional(Schema.Number),
  error: Schema.optional(UnknownRecord),
  reason: Schema.optional(Schema.String),
  sessionId: Schema.String,
  terminal: Schema.String,
  turnId: Schema.String,
  usage: Schema.optional(Schema.Unknown),
  viewCursor: Schema.String,
});
export type TurnCompletedParams = typeof TurnCompletedParams.Type;

export const TurnStartedParams = Schema.Struct({
  commandId: Schema.String,
  sessionId: Schema.String,
  turnId: Schema.String,
  viewCursor: Schema.String,
});
export type TurnStartedParams = typeof TurnStartedParams.Type;

export const ItemLifecycleParams = Schema.Struct({
  item: UnknownRecord,
  sessionId: Schema.String,
  viewCursor: Schema.String,
});
export type ItemLifecycleParams = typeof ItemLifecycleParams.Type;

export const ItemDeltaParams = Schema.Struct({
  delta: Schema.String,
  field: Schema.optional(Schema.String),
  itemId: Schema.String,
  sessionId: Schema.String,
  viewCursor: Schema.String,
});
export type ItemDeltaParams = typeof ItemDeltaParams.Type;

export const ApprovalUpdatedParams = Schema.Struct({
  approvalId: Schema.String,
  availableChoices: Schema.Array(ApprovalChoice),
  currentRequirementId: Schema.Struct({
    approvalId: Schema.String,
    sourceIndex: Schema.Number,
  }),
  sessionId: Schema.String,
  viewCursor: Schema.String,
});
export type ApprovalUpdatedParams = typeof ApprovalUpdatedParams.Type;

export const ApprovalResolvedParams = Schema.Struct({
  approvalId: Schema.String,
  decision: Schema.String,
  itemId: Schema.String,
  sessionId: Schema.String,
  turnId: Schema.String,
  viewCursor: Schema.String,
});
export type ApprovalResolvedParams = typeof ApprovalResolvedParams.Type;

export const UserInputSettledParams = Schema.Struct({
  outcome: Schema.String,
  sessionId: Schema.String,
  userInputId: Schema.String,
  viewCursor: Schema.String,
});
export type UserInputSettledParams = typeof UserInputSettledParams.Type;

export const ViewGapParams = Schema.Struct({
  after: Schema.optional(Schema.String),
  sessionId: Schema.optional(Schema.String),
});
export type ViewGapParams = typeof ViewGapParams.Type & Record<string, unknown>;
