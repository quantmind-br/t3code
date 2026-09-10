/**
 * MuseAdapter — ProviderAdapterShape implementation for Muse Code, over the
 * Muse Session Protocol (MSP) via `packages/effect-msp`.
 *
 * Lifecycle: one `muse serve` host process per T3 thread, kept alive for the
 * life of the session (never respawned per turn) so the native session log
 * — and whatever prompt-cache-friendly state the host/model keep tied to it
 * — survives across turns. `startSession` either starts a fresh MSP session
 * (`session/start`) or resumes a persisted one (`session/resume`) using the
 * `{driverVersion, museSessionId, viewCursor}` cursor stored in
 * `ProviderSession.resumeCursor`. Turns, interrupts, approvals, structured
 * user-input, model listing, and native compaction all go over the same
 * connection; the host's own event log is treated as the source of native
 * conversation continuity, and this adapter never resends prior turns to
 * fake continuation.
 *
 * Attachments: images ride the turn as native MSP `image` input parts
 * (base64 + `mediaType`); files and unknown types reach the model through the
 * on-disk path line `ProviderService` appends to the prompt, which is also
 * MSP's own mechanism for file mentions.
 *
 * Subagent and workflow items are not rendered as generic tool calls: they
 * map to `collab_agent_tool_call` and are additionally projected onto the
 * task lifecycle (`task.started`/`task.progress`/`task.completed`) with real
 * agent identity, role, model, phases and usage. A workflow's folded
 * `children` each become their own task row parented to the workflow — see
 * `emitAgentTaskEvents`.
 *
 * A `view/gap` bracket is recovered by splice-fill through `view/page`, MSP's
 * own sanctioned recovery — see `fillViewGap`.
 *
 * Per-instance account isolation is driven by the `homePath` setting, which
 * pins `XDG_CONFIG_HOME`/`XDG_DATA_HOME`/`MUSE_AUTH_PATH` for every process
 * this adapter spawns — see `MuseEnvironment`.
 *
 * Deliberately out of scope for this pass (tracked as a follow-up, not
 * silently dropped): `readThread`, which serves this adapter's own in-memory
 * item cache (seeded from resume history, then live notifications) grouped by
 * turn rather than a fresh `session/read` query — sufficient for feedback
 * upload and diagnostics, not a substitute for the native session log.
 * Conversation rollback IS supported, via `session/fork` — see
 * `rollbackThread` for the exact semantics and its one limit (cannot rewind
 * past the first turn).
 *
 * @module provider/Layers/MuseAdapter
 */
import {
  type ApprovalRequestId,
  EventId,
  type MuseSettings,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type RuntimeMode,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  RuntimeRequestId,
  RuntimeTaskId,
  type RuntimeTaskStatus,
  type RuntimeTaskUsage,
  ThreadId,
  type ProviderTurnStartResult,
  TurnId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as MspClient from "effect-msp/client";
import * as MspError from "effect-msp/errors";
import * as MspSchema from "effect-msp/schema";
import { randomUuidV7 } from "effect-msp/uuid";

import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape, ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";
import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { makeMuseEnvironment } from "./MuseEnvironment.ts";

const PROVIDER = ProviderDriverKind.make("muse");
const RESUME_CURSOR_VERSION = 1 as const;

/**
 * `view/gap` splice-fill budget. MSP caps `view/page` at 1000 events per call;
 * 200 keeps one recovery request small enough not to stall the notification
 * consumer (the fill runs on it), and 25 pages bounds a pathological gap at
 * 5,000 recovered events before the adapter gives up and says so.
 */
const VIEW_GAP_PAGE_LIMIT = 200;
const VIEW_GAP_MAX_PAGES = 25;

/**
 * How many recently dispatched view cursors a thread remembers. Only needs to
 * outlast a gap bracket, and one bracket is bounded by the recovery budget
 * above (200 x 25), so this covers a worst-case gap twice over while keeping
 * the set trivially small for a long session.
 */
const DISPATCHED_CURSOR_MEMORY = 10_000;

interface MuseResumeCursor {
  readonly schemaVersion: typeof RESUME_CURSOR_VERSION;
  readonly museSessionId: string;
  readonly viewCursor?: string;
}

/**
 * `undefined` means no cursor was supplied at all (start fresh is correct).
 * `"invalid"` means one WAS supplied but doesn't parse — callers must fail
 * loudly rather than silently falling back to `session/start`, which would
 * quietly abandon the original Muse session and rebind the thread to a new
 * one underneath an unsuspecting UI.
 */
function parseMuseResumeCursor(raw: unknown): MuseResumeCursor | undefined | "invalid" {
  if (raw === undefined || raw === null) return undefined;
  if (typeof raw !== "object" || Array.isArray(raw)) return "invalid";
  const record = raw as Record<string, unknown>;
  if (record.schemaVersion !== RESUME_CURSOR_VERSION) return "invalid";
  if (typeof record.museSessionId !== "string" || record.museSessionId.trim().length === 0) {
    return "invalid";
  }
  return {
    schemaVersion: RESUME_CURSOR_VERSION,
    museSessionId: record.museSessionId,
    ...(typeof record.viewCursor === "string" ? { viewCursor: record.viewCursor } : {}),
  };
}

interface AnsweredUserInput {
  readonly pending: MspSchema.UserInputRequestParams;
  readonly answers: ProviderUserInputAnswers;
}

interface PendingApproval {
  readonly availableChoices: ReadonlyArray<MspSchema.ApprovalChoice>;
  readonly currentRequirementId: MspSchema.ApprovalRequirementRef;
  readonly sessionId: string;
  readonly turnId: string;
  readonly itemId: string;
  readonly toolName: string;
  readonly rawArgs: string;
  readonly subject: Record<string, unknown>;
}

interface MuseThreadState {
  readonly threadId: ThreadId;
  readonly scope: Scope.Closeable;
  readonly client: MspClient.MspClient;
  /** A Ref, not a constant: `rollbackThread` forks onto a NEW Muse session
   * (MSP has no in-place rewind), and every later call on this thread must
   * target that fork — see `rollbackThread`. */
  readonly museSessionId: Ref.Ref<string>;
  readonly cwd: string | undefined;
  readonly viewCursor: Ref.Ref<string>;
  readonly activeTurnId: Ref.Ref<Option.Option<TurnId>>;
  readonly pendingApprovals: Ref.Ref<Map<string, PendingApproval>>;
  readonly pendingUserInputs: Ref.Ref<Map<string, MspSchema.UserInputRequestParams>>;
  /** Requests this adapter itself already answered (via `respondToUserInput`),
   * awaiting the host's own `userInput/settled` notification. Keeping the
   * real answers here — rather than emitting `user-input.resolved` eagerly
   * — means settlement is reported exactly once, from one place, whether it
   * originated locally or from a timeout/interruption/another client. */
  readonly pendingAnsweredUserInputs: Ref.Ref<Map<string, AnsweredUserInput>>;
  /** Best-effort item cache for `readThread`; not the native session log. */
  readonly items: Ref.Ref<Array<MspSchema.Item>>;
  readonly itemKindByItemId: Ref.Ref<Map<string, string>>;
  /** Task rows this thread already opened, so a re-emitted item revision (or a
   * `view/gap` fill that replays an item the live stream already delivered)
   * updates the row instead of opening a second one. */
  readonly emittedTaskIds: Ref.Ref<Set<string>>;
  /** Task rows already closed, so a terminal item re-emitted at a higher
   * revision does not report a second completion. */
  readonly completedTaskIds: Ref.Ref<Set<string>>;
  /** Highest item revision already projected onto a task row, per item id, so
   * a stale revision arriving after a newer one (recovery racing the live
   * stream) cannot walk the task backwards. */
  readonly taskItemRevision: Ref.Ref<Map<string, number>>;
  /** Bounded FIFO of view cursors this thread already dispatched. `view/gap`
   * recovery stops at the first one it meets: that proves the walk reached
   * territory the live stream already delivered. Insertion-ordered, capped at
   * `DISPATCHED_CURSOR_MEMORY` so a long session cannot grow it without
   * bound. */
  readonly dispatchedViewCursors: Ref.Ref<Set<string>>;
  /** Characters of each item's streamed field already published, so a durable
   * re-emission (notably a `view/gap` fill) can publish only the suffix the
   * ephemeral delta stream never delivered. */
  readonly streamedContentChars: Ref.Ref<Map<string, number>>;
  readonly createdAt: string;
  readonly runtimeMode: Ref.Ref<RuntimeMode>;
  readonly modelId: Ref.Ref<string | undefined>;
  /** Serializes session-changing operations (turn start/interrupt, rollback,
   * compaction) on this thread. ProviderService offers no cross-operation
   * lock, and `activeTurnId` is only set asynchronously by `turn/started`, so
   * without this a `sendTurn` could slip in between rollback's read of the
   * source session and its switch to the replacement. */
  readonly sessionLock: Semaphore.Semaphore;
}

/**
 * Maps T3's coarse `RuntimeMode` onto MSP's `ApprovalMode`. Best-effort:
 * MSP has 4 modes to T3's 4, but the semantics don't line up 1:1
 * (`auto-accept-edits` has no direct MSP equivalent) — `promptUnmatched`
 * (auto-approve well-understood actions, prompt for the rest) is the
 * closest fit for both `auto` and `auto-accept-edits`.
 */
function runtimeModeToApprovalMode(mode: RuntimeMode): MspSchema.ApprovalMode {
  switch (mode) {
    case "full-access":
      return "allowAll";
    case "approval-required":
      return "onRequest";
    case "auto-accept-edits":
    case "auto":
      return "promptUnmatched";
  }
}

/**
 * Maps a T3 `ProviderApprovalDecision` to one of the native `availableChoices`
 * the host actually offered for this approval. Fails loudly (never silently
 * substitutes a different choice) when the host offers nothing in that
 * decision family — that is a real capability mismatch to surface, not a
 * client bug to paper over.
 */
function resolveMuseChoice(
  decision: ProviderApprovalDecision,
  availableChoices: ReadonlyArray<MspSchema.ApprovalChoice>,
): MspSchema.ApprovalChoice | undefined {
  const family: ReadonlyArray<string> = (() => {
    switch (decision) {
      case "accept":
        return ["approved"];
      case "acceptForSession":
        return ["approvedForSession", "approved"];
      case "acceptAlways":
        return ["approvedPolicyAmendment", "approvedForSession", "approved"];
      case "decline":
        return ["denied", "deniedPolicyAmendment"];
      case "cancel":
        return ["abort", "timedOut", "denied"];
    }
  })();
  for (const wanted of family) {
    const match = availableChoices.find((choice) => choice.decision === wanted);
    if (match) return match;
  }
  return undefined;
}

const canonicalItemType = (item: MspSchema.Item): string => {
  switch (item.kind) {
    case "userMessage":
      return "user_message";
    case "agentMessage":
      return "assistant_message";
    case "reasoning":
      return "reasoning";
    case "compaction":
      return "context_compaction";
    case "userShell":
      return "command_execution";
    case "subagent":
    case "workflow":
      // Agent-delegation work, not an ordinary tool call: this is what puts
      // these items in the Agents surface and the work log's agent-tool lane
      // instead of rendering them as a generic tool row.
      return "collab_agent_tool_call";
    case "toolCall":
    case "reminderChild":
      return "dynamic_tool_call";
    default:
      return "unknown";
  }
};

const itemTitle = (item: MspSchema.Item): string | undefined => {
  if (typeof item.toolName === "string") return item.toolName;
  if (typeof item.commandText === "string") return item.commandText;
  // Delegated work is identified by what it was asked to do, not by a tool
  // name it does not have.
  if (item.kind === "subagent") {
    if (typeof item.objective === "string" && item.objective.trim()) return item.objective.trim();
    if (typeof item.role === "string" && item.role.trim()) return item.role.trim();
    if (typeof item.agentPath === "string" && item.agentPath.trim()) return item.agentPath.trim();
  }
  if (item.kind === "workflow") {
    if (typeof item.entryId === "string" && item.entryId.trim()) return item.entryId.trim();
    if (typeof item.scriptId === "string" && item.scriptId.trim()) return item.scriptId.trim();
  }
  return undefined;
};

/** Trimmed non-empty string, or `undefined` — the shape every optional runtime field wants. */
const optionalText = (value: unknown): string | undefined => {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
};

const optionalNonNegativeInt = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : undefined;

/**
 * MSP `TokenUsage` -> `RuntimeTaskUsage`. MSP reports input/output/cached/
 * reasoning separately and has no total, so the total is derived; the cache
 * read/write split collapses into `cachedInputTokens` because the runtime
 * contract only models one cached bucket.
 */
const toRuntimeTaskUsage = (usage: unknown, durationMs?: unknown): RuntimeTaskUsage | undefined => {
  if (typeof usage !== "object" || usage === null) {
    const onlyDuration = optionalNonNegativeInt(durationMs);
    return onlyDuration === undefined ? undefined : { totalTokens: 0, durationMs: onlyDuration };
  }
  const record = usage as Record<string, unknown>;
  const inputTokens = optionalNonNegativeInt(record.inputTokens);
  const outputTokens = optionalNonNegativeInt(record.outputTokens);
  const cachedInputTokens = optionalNonNegativeInt(record.cachedTokens);
  const reasoningOutputTokens = optionalNonNegativeInt(record.reasoningTokens);
  const duration = optionalNonNegativeInt(durationMs);
  return {
    totalTokens: (inputTokens ?? 0) + (outputTokens ?? 0),
    ...(inputTokens !== undefined ? { inputTokens } : {}),
    ...(outputTokens !== undefined ? { outputTokens } : {}),
    ...(cachedInputTokens !== undefined ? { cachedInputTokens } : {}),
    ...(reasoningOutputTokens !== undefined ? { reasoningOutputTokens } : {}),
    ...(duration !== undefined ? { durationMs: duration } : {}),
  };
};

/**
 * MSP `ItemStatus` (open enum, terminal = anything but `inProgress`) plus the
 * `subagent`-only `controlStatus` lane, mapped onto the runtime task
 * vocabulary. Unknown values are treated as terminal-unknown per MSP's
 * rendering rule, which on this side means `failed` rather than a lie about
 * success.
 */
const MSP_ITEM_STATUS_TO_TASK_STATUS: Record<string, RuntimeTaskStatus> = {
  inProgress: "running",
  completed: "completed",
  failed: "failed",
  cancelled: "cancelled",
  rejected: "failed",
  timedOut: "failed",
};

const MSP_CONTROL_STATUS_TO_TASK_STATUS: Record<string, RuntimeTaskStatus> = {
  accepted: "pending",
  starting: "pending",
  running: "running",
  resultReady: "running",
  closing: "running",
  closed: "completed",
  recoveryPending: "waiting",
  manualReconciliation: "waiting",
};

const taskStatusForItem = (item: MspSchema.Item): RuntimeTaskStatus => {
  const status = typeof item.status === "string" ? item.status : "";
  // `status` is the generic item vocabulary and wins whenever it is terminal;
  // `controlStatus` only refines the still-open subagent lifecycle.
  if (status !== "" && status !== "inProgress") {
    return MSP_ITEM_STATUS_TO_TASK_STATUS[status] ?? "failed";
  }
  const control = typeof item.controlStatus === "string" ? item.controlStatus : undefined;
  if (control) return MSP_CONTROL_STATUS_TO_TASK_STATUS[control] ?? "running";
  return MSP_ITEM_STATUS_TO_TASK_STATUS[status] ?? "running";
};

/** Terminal task statuses collapse to the three the completed payload allows. */
const toCompletedStatus = (status: RuntimeTaskStatus): "completed" | "failed" | "stopped" => {
  if (status === "completed") return "completed";
  if (status === "cancelled" || status === "interrupted") return "stopped";
  return "failed";
};

/**
 * The workflow's phase list, in first-appearance order over its children. MSP
 * carries a phase name per child and no ordered phase table, so the order the
 * children were folded in is the only ordering signal available.
 */
const workflowPhases = (
  children: ReadonlyArray<MspSchema.WorkflowChild>,
): Array<{ readonly index: number; readonly title: string }> => {
  const seen = new Set<string>();
  const phases: Array<{ readonly index: number; readonly title: string }> = [];
  for (const child of children) {
    const title = optionalText(child.phase);
    if (title === undefined || seen.has(title)) continue;
    seen.add(title);
    phases.push({ index: phases.length, title });
  }
  return phases;
};

/** MSP `WorkflowChild.status` is camelCased durable runtime vocabulary, not the item enum. */
const MSP_WORKFLOW_CHILD_STATUS_TO_TASK_STATUS: Record<string, RuntimeTaskStatus> = {
  pending: "pending",
  queued: "pending",
  scheduled: "pending",
  starting: "pending",
  running: "running",
  waiting: "waiting",
  paused: "idle",
  completed: "completed",
  succeeded: "completed",
  failed: "failed",
  cancelled: "cancelled",
  skipped: "cancelled",
};

const workflowChildStatus = (child: MspSchema.WorkflowChild): RuntimeTaskStatus => {
  // `terminal` uses the turn vocabulary and is authoritative once present.
  const terminal = optionalText(child.terminal);
  if (terminal === "completed") return "completed";
  if (terminal === "cancelled") return "cancelled";
  if (terminal === "failed") return "failed";
  if (terminal !== undefined) return "failed";
  const status = optionalText(child.status);
  if (status === undefined) return "running";
  return MSP_WORKFLOW_CHILD_STATUS_TO_TASK_STATUS[status] ?? "running";
};

const TERMINAL_TASK_STATUSES: ReadonlySet<RuntimeTaskStatus> = new Set([
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);

const itemDetail = (item: MspSchema.Item): string | undefined =>
  typeof item.fallbackText === "string" ? item.fallbackText : undefined;

/** Defensively narrows a raw `SessionHistory.items` array to well-formed items. */
const decodeHistoryItems = (raw: ReadonlyArray<unknown>): Array<MspSchema.Item> => {
  const items: Array<MspSchema.Item> = [];
  for (const entry of raw) {
    if (typeof entry !== "object" || entry === null) continue;
    const record = entry as Record<string, unknown>;
    if (typeof record.itemId !== "string" || typeof record.kind !== "string") continue;
    items.push(record as MspSchema.Item);
  }
  return items;
};

/**
 * Distinct `turnId`s in first-appearance order. History items are served in
 * fold order, so this is the session's chronological turn list. `userShell`
 * items carry no turn (the one kind outside a turn per MSP) and are skipped.
 */
const orderedTurnIds = (items: ReadonlyArray<MspSchema.Item>): Array<string> => {
  const seen = new Set<string>();
  const ordered: Array<string> = [];
  for (const item of items) {
    const turnId = item.turnId;
    if (typeof turnId !== "string" || seen.has(turnId)) continue;
    seen.add(turnId);
    ordered.push(turnId);
  }
  return ordered;
};

/** Groups items into `ProviderThreadSnapshot` turns; turn-less items go under a synthetic id. */
const groupItemsByTurn = (
  items: ReadonlyArray<MspSchema.Item>,
): Array<{ readonly id: TurnId; readonly items: ReadonlyArray<unknown> }> => {
  const byTurn = new Map<string, Array<MspSchema.Item>>();
  for (const item of items) {
    const key = typeof item.turnId === "string" ? item.turnId : "muse-outside-turn";
    const bucket = byTurn.get(key);
    if (bucket) bucket.push(item);
    else byTurn.set(key, [item]);
  }
  return Array.from(byTurn, ([id, turnItems]) => ({ id: TurnId.make(id), items: turnItems }));
};

export interface MuseAdapterOptions {
  readonly binaryPath: string;
  readonly customModels: MuseSettings["customModels"];
  readonly environment: NodeJS.ProcessEnv;
  readonly instanceId: ProviderInstanceId;
  /** Per-instance Muse profile root; see `MuseEnvironment`. Empty shares the default account. */
  readonly homePath: string | undefined;
  /** Where `ProviderService` stores uploaded attachments, read when sending image parts. */
  readonly attachmentsDir: string;
}

export const makeMuseAdapter = Effect.fn("makeMuseAdapter")(function* (
  options: MuseAdapterOptions,
): Effect.fn.Return<
  ProviderAdapterShape<ProviderAdapterError>,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem
> {
  const fileSystem = yield* FileSystem.FileSystem;
  // One environment for every child this adapter spawns, so all of them agree
  // on which Muse account and session store this instance owns.
  const environment = makeMuseEnvironment(options.environment, options.homePath);
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const threads = yield* Ref.make(new Map<ThreadId, MuseThreadState>());
  const runtimeEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const nextEventId = Effect.map(randomUuidV7, (id) => EventId.make(id));
  const stamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

  const emit = (event: ProviderRuntimeEvent) =>
    PubSub.publish(runtimeEvents, event).pipe(Effect.asVoid);

  const emitBase = (input: { readonly threadId: ThreadId; readonly turnId?: TurnId }) =>
    Effect.map(stamp(), (base) => ({ provider: PROVIDER, ...base, ...input }));

  const requireThread = (threadId: ThreadId) =>
    Ref.get(threads).pipe(
      Effect.flatMap((current) => {
        const state = current.get(threadId);
        return state
          ? Effect.succeed(state)
          : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
      }),
    );

  /** `requireThread` + run `body` holding that thread's `sessionLock`. */
  const withThreadLock = <A, E>(
    threadId: ThreadId,
    body: (state: MuseThreadState) => Effect.Effect<A, E>,
  ): Effect.Effect<A, E | ProviderAdapterSessionNotFoundError> =>
    requireThread(threadId).pipe(
      Effect.flatMap((state) => state.sessionLock.withPermit(body(state))),
    );

  const mapMspError = (threadId: ThreadId, operation: string) => (cause: MspError.MspError) => {
    if (cause._tag === "MspRequestError") {
      return new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: operation,
        detail: cause.errorMessage,
        cause,
      });
    }
    return new ProviderAdapterProcessError({
      provider: PROVIDER,
      threadId,
      detail: cause.message,
      cause,
    });
  };

  /**
   * Reads one uploaded image attachment off disk and turns it into an MSP
   * `image` input part. MSP takes base64 inline (there is no upload handle),
   * and `mediaType` is required on an image part.
   */
  const readImageInputPart = (
    threadId: ThreadId,
    attachment: NonNullable<ProviderSendTurnInput["attachments"]>[number],
  ): Effect.Effect<MspSchema.TurnInputPart, ProviderAdapterError> =>
    Effect.gen(function* () {
      const attachmentPath = resolveAttachmentPath({
        attachmentsDir: options.attachmentsDir,
        attachment,
      });
      if (!attachmentPath) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "turn/start",
          detail: `Invalid attachment id '${attachment.id}'.`,
        });
      }
      const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "turn/start",
              detail: `Failed to read attachment file: ${cause.message}.`,
              cause,
            }),
        ),
      );
      return {
        type: "image" as const,
        base64Data: Buffer.from(bytes).toString("base64"),
        mediaType: attachment.mimeType,
      } satisfies MspSchema.TurnInputPart;
    }).pipe(Effect.withSpan("MuseAdapter.readImageInputPart", { attributes: { threadId } }));

  /**
   * Projects a `subagent` or `workflow` item onto the task lifecycle.
   *
   * MSP has no task protocol: delegated work is only ever visible as transcript
   * items whose whole state is re-emitted at a higher revision. So the mapping
   * is idempotent-by-construction — `task.started` fires once per item id, and
   * every later revision is a `task.progress` (with the folded status) plus a
   * `task.completed` on the terminal revision. `emittedTaskIds` is what keeps
   * a re-emitted item from opening a second row, including after a `view/gap`
   * fill replays an item the live stream already delivered.
   *
   * A `workflow` item additionally carries its children folded whole; each one
   * becomes its own task row parented to the workflow, which is the only way
   * per-child phase/attempt/usage survives into the Agents surface.
   */
  const emitAgentTaskEvents = (
    state: MuseThreadState,
    item: MspSchema.Item,
    method: "item/started" | "item/updated" | "item/completed",
    activeTurnId: Option.Option<TurnId>,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      if (item.kind !== "subagent" && item.kind !== "workflow") return;

      const turnFields = Option.isSome(activeTurnId) ? { turnId: activeTurnId.value } : {};
      const emitTask = (event: Record<string, unknown>) =>
        Effect.gen(function* () {
          const base = yield* emitBase({ threadId: state.threadId });
          yield* emit({ ...base, ...turnFields, ...event } as ProviderRuntimeEvent);
        });

      const isWorkflow = item.kind === "workflow";
      const children = Array.isArray(item.children)
        ? (item.children as ReadonlyArray<MspSchema.WorkflowChild>)
        : [];
      const phases = isWorkflow ? workflowPhases(children) : [];
      const description =
        itemTitle(item) ??
        optionalText(item.fallbackText) ??
        (isWorkflow ? "Workflow" : "Subagent");
      const workflowRunId = optionalText(item.workflowRunId);
      const childSessionLogPath = optionalText(item.childSessionLogPath);
      const scriptId = optionalText(item.scriptId);
      const runHandles = {
        ...(workflowRunId ? { runId: workflowRunId } : {}),
        ...(scriptId ? { scriptPath: scriptId } : {}),
        ...(childSessionLogPath ? { transcriptDir: childSessionLogPath } : {}),
      };
      // `childSessionId` is the drill-down handle into the child's own
      // transcript (`session/read`/`view/page`); surfacing it as the agent id
      // is what lets a client ask for that transcript later.
      const agentId = optionalText(item.subagentId) ?? optionalText(item.childSessionId);
      const linkage = {
        taskType: isWorkflow ? "local_workflow" : "subagent",
        agentKind: "agent" as const,
        ...(agentId ? { agentId } : {}),
        title: description,
        ...(optionalText(item.role) ? { role: optionalText(item.role) } : {}),
        ...(optionalText(item.agentPath) ? { agentPath: optionalText(item.agentPath) } : {}),
        ...(isWorkflow && (optionalText(item.entryId) ?? scriptId)
          ? { workflowName: optionalText(item.entryId) ?? scriptId }
          : {}),
        ...(phases.length > 0 ? { phases } : {}),
        ...(Object.keys(runHandles).length > 0 ? { runHandles } : {}),
        ...(childSessionLogPath ? { outputFile: childSessionLogPath } : {}),
        ...(optionalNonNegativeInt(item.depth) !== undefined
          ? { agentIndex: optionalNonNegativeInt(item.depth) }
          : {}),
      };

      const taskId = RuntimeTaskId.make(item.itemId);
      const status = taskStatusForItem(item);
      const usage = toRuntimeTaskUsage(item.usage, item.durationMs);
      const failure = optionalText(item.failureReason);
      const summary = optionalText(item.message) ?? optionalText(item.fallbackText);

      // MSP's apply rule for items is replace-iff-higher on `revision`, and a
      // `view/gap` fill runs interleaved with the live stream, so a stale
      // revision can arrive after a newer one. Dropping it here is what stops
      // an older `inProgress` revision from reopening a task the newer
      // terminal revision already closed — which would otherwise leave the row
      // stuck running forever, since its completion is deduped away.
      const revision = optionalNonNegativeInt(item.revision);
      const isStale = yield* Ref.modify(state.taskItemRevision, (current) => {
        if (revision === undefined) return [false, current] as const;
        const seen = current.get(item.itemId);
        if (seen !== undefined && revision < seen) return [true, current] as const;
        const next = new Map(current);
        next.set(item.itemId, revision);
        return [false, next] as const;
      });
      if (isStale) return;

      const alreadyStarted = yield* Ref.modify(state.emittedTaskIds, (current) => {
        if (current.has(item.itemId)) return [true, current] as const;
        const next = new Set(current);
        next.add(item.itemId);
        return [false, next] as const;
      });

      if (!alreadyStarted) {
        yield* emitTask({
          type: "task.started",
          payload: { taskId, description, ...linkage },
        });
      }

      const alreadyClosed = (yield* Ref.get(state.completedTaskIds)).has(item.itemId);
      const terminal =
        alreadyClosed || method === "item/completed" || TERMINAL_TASK_STATUSES.has(status);
      if (!terminal) {
        yield* emitTask({
          type: "task.progress",
          payload: {
            taskId,
            description,
            status,
            ...(summary ? { summary } : {}),
            ...(usage ? { typedUsage: usage } : {}),
            ...(failure ? { error: failure } : {}),
            ...linkage,
          },
        });
      }

      // Children first: a client folding the workflow's terminal row should
      // already have every child's final state in hand.
      yield* Effect.forEach(
        children,
        (child) => emitWorkflowChildEvents(state, item, child, turnFields, phases),
        { concurrency: 1, discard: true },
      );

      if (!terminal) return;
      yield* Ref.update(state.completedTaskIds, (current) => {
        if (current.has(item.itemId)) return current;
        const next = new Set(current);
        next.add(item.itemId);
        return next;
      });
      if (alreadyClosed) {
        // The row is already closed, but a higher terminal revision can still
        // carry a final summary or failure reason the first one lacked.
        // `task.updated` forwards that without reporting a second lifecycle
        // transition or reopening the task.
        //
        // Usage is deliberately NOT forwarded here: `TaskUpdatedPayload` has no
        // usage field, and the only payloads that do (`task.progress`,
        // `task.completed`) would respectively reopen or re-close the row. A
        // late-arriving usage-only revision is therefore dropped rather than
        // faked through a lifecycle transition that did not happen.
        const enrichment = {
          ...((summary ?? failure) ? { description: summary ?? failure } : {}),
          ...(failure ? { error: failure } : {}),
        };
        if (Object.keys(enrichment).length === 0) return;
        yield* emitTask({
          type: "task.updated",
          payload: {
            taskId,
            status,
            ...enrichment,
            ...linkage,
          },
        });
        return;
      }
      yield* emitTask({
        type: "task.completed",
        payload: {
          taskId,
          status: toCompletedStatus(status),
          ...((summary ?? failure) ? { summary: summary ?? failure } : {}),
          ...(usage ? { typedUsage: usage } : {}),
          ...linkage,
        },
      });
    });

  /**
   * One workflow child as its own task row. The `(childId, attempt)` pair is
   * the child's identity in MSP, so it is also the task id — a retried child
   * is a distinct row rather than a mutated one.
   */
  const emitWorkflowChildEvents = (
    state: MuseThreadState,
    parent: MspSchema.Item,
    child: MspSchema.WorkflowChild,
    turnFields: { readonly turnId?: TurnId },
    phases: ReadonlyArray<{ readonly index: number; readonly title: string }>,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const childKey = `${parent.itemId}:${child.childId}:${child.attempt}`;
      const taskId = RuntimeTaskId.make(childKey);
      const description = optionalText(child.label) ?? child.childId;
      const phaseTitle = optionalText(child.phase);
      const phaseIndex = phaseTitle
        ? phases.find((phase) => phase.title === phaseTitle)?.index
        : undefined;
      const linkage = {
        taskType: "local_workflow_child",
        agentKind: "agent" as const,
        agentId: childKey,
        parentAgentId:
          optionalText(parent.subagentId) ?? optionalText(parent.childSessionId) ?? parent.itemId,
        title: description,
        ...((optionalText(parent.entryId) ?? optionalText(parent.scriptId))
          ? { workflowName: optionalText(parent.entryId) ?? optionalText(parent.scriptId) }
          : {}),
        ...(phaseTitle ? { phaseTitle } : {}),
        ...(phaseIndex !== undefined ? { phaseIndex } : {}),
        ...(optionalNonNegativeInt(child.attempt) !== undefined
          ? { attempt: optionalNonNegativeInt(child.attempt) }
          : {}),
        ...(phases.length > 0 ? { phases } : {}),
      };

      const emitTask = (event: Record<string, unknown>) =>
        Effect.gen(function* () {
          const base = yield* emitBase({ threadId: state.threadId });
          yield* emit({ ...base, ...turnFields, ...event } as ProviderRuntimeEvent);
        });

      const status = workflowChildStatus(child);
      const usage = toRuntimeTaskUsage(child.usage, child.durationMs);

      const alreadyStarted = yield* Ref.modify(state.emittedTaskIds, (current) => {
        if (current.has(childKey)) return [true, current] as const;
        const next = new Set(current);
        next.add(childKey);
        return [false, next] as const;
      });
      if (!alreadyStarted) {
        yield* emitTask({
          type: "task.started",
          payload: { taskId, description, ...linkage },
        });
      }

      // Same closed-row rule as the parent: once a child row is terminal, a
      // re-emitted non-terminal state must not reopen it. A workflow item is
      // re-emitted whole on every change, so an already-finished child rides
      // along on every later revision.
      const alreadyClosed = (yield* Ref.get(state.completedTaskIds)).has(childKey);
      if (!alreadyClosed && !TERMINAL_TASK_STATUSES.has(status)) {
        yield* emitTask({
          type: "task.progress",
          payload: {
            taskId,
            description,
            status,
            ...(usage ? { typedUsage: usage } : {}),
            ...linkage,
          },
        });
        return;
      }
      if (!TERMINAL_TASK_STATUSES.has(status)) return;

      yield* Ref.update(state.completedTaskIds, (current) => {
        if (current.has(childKey)) return current;
        const next = new Set(current);
        next.add(childKey);
        return next;
      });
      const summary = optionalText(child.resultRef);
      if (alreadyClosed) {
        // Enrichment only, and only what `TaskUpdatedPayload` can express: a
        // later revision may attach the child's result reference. Usage has no
        // field on that payload (see the parent branch), so it is dropped
        // rather than routed through a lifecycle event that did not happen.
        if (summary === undefined) return;
        yield* emitTask({
          type: "task.updated",
          payload: {
            taskId,
            status,
            ...(summary ? { description: summary } : {}),
            ...linkage,
          },
        });
        return;
      }
      yield* emitTask({
        type: "task.completed",
        payload: {
          taskId,
          status: toCompletedStatus(status),
          ...(summary ? { summary } : {}),
          ...(usage ? { typedUsage: usage } : {}),
          ...linkage,
        },
      });
    });

  /** Consumes one host's notification stream for the lifetime of its scope. */
  const attachNotificationConsumer = (state: MuseThreadState) =>
    state.client.notifications.pipe(
      Stream.runForEach((notification) => handleNotification(state, notification)),
      Effect.catchCause((cause) =>
        // Closing `state.scope` (an intentional `stopSession`/`stopAll`/host
        // replacement) interrupts this consumer too — that is not a transport
        // failure and must not be reported as one, or runtime ingestion
        // treats an ordinary stop as a provider crash and clears turn state.
        Cause.hasInterruptsOnly(cause)
          ? Effect.void
          : Effect.gen(function* () {
              const base = yield* emitBase({ threadId: state.threadId });
              // The host connection is gone: nothing more will ever come from it,
              // so drop the thread from the live map now rather than leaving
              // `hasSession`/`listSessions` report a session that can no longer
              // accept turns or answer approvals.
              yield* Ref.update(threads, (current) => {
                const next = new Map(current);
                next.delete(state.threadId);
                return next;
              });
              yield* emit({
                ...base,
                type: "session.exited",
                payload: {
                  reason: "host connection closed",
                  recoverable: false,
                  exitKind: "error",
                },
                raw: { source: "muse.msp.notification", payload: String(cause) },
              } as ProviderRuntimeEvent);
              // LAST, after every emit above: closing `state.scope` interrupts
              // this very fiber (it was forked into that scope), so any yield
              // point after this line would unwind and silently drop its work.
              // Same pattern as OpenCodeAdapter's session-exit teardown. This
              // is what actually kills an orphaned `muse serve` whose
              // connection died on a protocol-level parse/write error while
              // the OS process itself is still alive.
              yield* Scope.close(state.scope, Exit.void).pipe(Effect.ignore);
            }),
      ),
      Effect.forkIn(state.scope),
    );

  /**
   * Records one view notification's cursor as dispatched, evicting oldest-first
   * past `DISPATCHED_CURSOR_MEMORY`. `Set` iterates in insertion order, which
   * is the arrival order, which — cursors being strictly monotonic — is also
   * cursor order, so the eviction always drops the oldest positions.
   */
  const rememberDispatchedCursor = (
    state: MuseThreadState,
    params: unknown,
  ): Effect.Effect<void> => {
    if (typeof params !== "object" || params === null) return Effect.void;
    const viewCursor = (params as { readonly viewCursor?: unknown }).viewCursor;
    if (typeof viewCursor !== "string" || viewCursor.length === 0) return Effect.void;
    return Ref.update(state.dispatchedViewCursors, (current) => {
      if (current.has(viewCursor)) return current;
      const next = new Set(current);
      next.add(viewCursor);
      while (next.size > DISPATCHED_CURSOR_MEMORY) {
        const oldest = next.values().next();
        if (oldest.done === true) break;
        next.delete(oldest.value);
      }
      return next;
    });
  };

  /** Records how many characters of an item's streamed field already shipped. */
  const noteStreamedContent = (
    state: MuseThreadState,
    itemId: string,
    delta: string,
  ): Effect.Effect<void> =>
    Ref.update(state.streamedContentChars, (current) =>
      new Map(current).set(itemId, (current.get(itemId) ?? 0) + delta.length),
    );

  /**
   * MSP splits an item's visible text across two surfaces: the durable item
   * carries the accumulated value, and `item/delta` streams it. Only the
   * stream reaches ingestion normally — but `item/delta` is ephemeral and
   * `view/page` serves durable events only, so a `view/gap` fill recovers the
   * item without a single character of its text.
   *
   * These fields are append-only per MSP's own delta contract (a delta appends
   * by field path), so the missing part is exactly the suffix past what the
   * stream already delivered. Emitting that suffix is a no-op on the live path
   * (where the counter already equals the full length) and is what actually
   * restores a dropped reply after a gap.
   *
   * `reasoning.summary` is deliberately not reconciled: it is an array whose
   * parts stream under per-index field paths, so a single character counter
   * cannot place the suffix, and reasoning is never the model's answer.
   */
  const DURABLE_CONTENT_FIELD: Record<string, "text" | "visibleOutput"> = {
    agentMessage: "text",
    toolCall: "visibleOutput",
    userShell: "visibleOutput",
  };

  const DURABLE_CONTENT_STREAM_KIND: Record<string, "assistant_text" | "command_output"> = {
    agentMessage: "assistant_text",
    toolCall: "command_output",
    userShell: "command_output",
  };

  const reconcileItemContent = (
    state: MuseThreadState,
    item: MspSchema.Item,
    base: Record<string, unknown>,
    activeTurnId: Option.Option<TurnId>,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const field = DURABLE_CONTENT_FIELD[item.kind];
      if (field === undefined) return;
      const full = item[field];
      if (typeof full !== "string" || full.length === 0) return;

      const missing = yield* Ref.modify(state.streamedContentChars, (current) => {
        const streamed = current.get(item.itemId) ?? 0;
        if (streamed >= full.length) return ["", current] as const;
        const next = new Map(current);
        next.set(item.itemId, full.length);
        return [full.slice(streamed), next] as const;
      });
      if (missing.length === 0) return;

      yield* emit({
        ...base,
        ...(Option.isSome(activeTurnId) ? { turnId: activeTurnId.value } : {}),
        itemId: RuntimeItemId.make(item.itemId),
        type: "content.delta",
        payload: {
          streamKind: DURABLE_CONTENT_STREAM_KIND[item.kind] ?? "unknown",
          delta: missing,
        },
      } as ProviderRuntimeEvent);
    });

  const handleNotification = (
    state: MuseThreadState,
    notification: MspClient.MspNotification,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const base = yield* emitBase({ threadId: state.threadId });
      const decode = <A, I>(schema: Schema.Codec<A, I>) =>
        Schema.decodeUnknownEffect(schema)(notification.params).pipe(Effect.option);

      // Remember this event's position before dispatching it. `view/gap`
      // recovery uses the record as its second stop condition, so it has to
      // cover every view notification — including kinds this adapter does not
      // map — or the walk could run past the hole into delivered territory.
      // `view/gap` itself carries no `viewCursor`, so it never lands here.
      yield* rememberDispatchedCursor(state, notification.params);

      switch (notification.method) {
        case "turn/started": {
          const params = yield* decode(MspSchema.TurnStartedParams);
          if (Option.isNone(params)) return;
          yield* Ref.set(state.activeTurnId, Option.some(TurnId.make(params.value.turnId)));
          yield* Ref.set(state.viewCursor, params.value.viewCursor);
          yield* emit({
            ...base,
            turnId: TurnId.make(params.value.turnId),
            type: "turn.started",
            payload: {},
          } as ProviderRuntimeEvent);
          return;
        }
        case "turn/completed": {
          const params = yield* decode(MspSchema.TurnCompletedParams);
          if (Option.isNone(params)) return;
          yield* Ref.set(state.activeTurnId, Option.none());
          yield* Ref.set(state.viewCursor, params.value.viewCursor);
          const errorMessage =
            typeof params.value.error?.message === "string"
              ? params.value.error.message
              : undefined;
          yield* emit({
            ...base,
            turnId: TurnId.make(params.value.turnId),
            type: "turn.completed",
            payload: {
              state:
                params.value.terminal === "completed" ||
                params.value.terminal === "failed" ||
                params.value.terminal === "cancelled"
                  ? params.value.terminal
                  : "failed",
              ...(errorMessage ? { errorMessage } : {}),
              ...(params.value.durationMs !== undefined
                ? { usage: undefined satisfies undefined, tokenUsage: undefined }
                : {}),
            },
          } as ProviderRuntimeEvent);
          return;
        }
        case "item/started":
        case "item/updated":
        case "item/completed": {
          const params = yield* decode(MspSchema.ItemLifecycleParams);
          if (Option.isNone(params)) return;
          const item = params.value.item as MspSchema.Item;
          yield* Ref.set(state.viewCursor, params.value.viewCursor);
          yield* Ref.update(state.itemKindByItemId, (current) =>
            new Map(current).set(item.itemId, item.kind),
          );
          yield* Ref.update(state.items, (current) => [
            ...current.filter((existing) => existing.itemId !== item.itemId),
            item,
          ]);
          const ITEM_LIFECYCLE_EVENT_TYPE = {
            "item/started": "item.started",
            "item/updated": "item.updated",
            "item/completed": "item.completed",
          } as const;
          const eventType =
            ITEM_LIFECYCLE_EVENT_TYPE[
              notification.method as keyof typeof ITEM_LIFECYCLE_EVENT_TYPE
            ];
          const activeTurnId = yield* Ref.get(state.activeTurnId);
          yield* emit({
            ...base,
            ...(Option.isSome(activeTurnId) ? { turnId: activeTurnId.value } : {}),
            itemId: RuntimeItemId.make(item.itemId),
            type: eventType,
            payload: {
              itemType: canonicalItemType(item),
              ...(itemTitle(item) ? { title: itemTitle(item) } : {}),
              ...(itemDetail(item) ? { detail: itemDetail(item) } : {}),
            },
          } as ProviderRuntimeEvent);
          // Durable text carried by the item itself, reconciled against what
          // the ephemeral delta stream already delivered. Live this is a
          // no-op; after a `view/gap` fill it is the only way the dropped
          // reply text reaches ingestion at all, because `view/page` serves
          // durable events only and `item/delta` is ephemeral.
          yield* reconcileItemContent(state, item, base, activeTurnId);
          // Subagent/workflow items are delegated agent work, not tool rows:
          // mirror them onto the task lifecycle so they land in the Agents
          // surface with real identity, role, model and usage instead of only
          // an opaque timeline entry.
          yield* emitAgentTaskEvents(state, item, notification.method, activeTurnId);
          return;
        }
        case "item/delta": {
          const params = yield* decode(MspSchema.ItemDeltaParams);
          if (Option.isNone(params)) return;
          yield* Ref.set(state.viewCursor, params.value.viewCursor);
          const kinds = yield* Ref.get(state.itemKindByItemId);
          const kind = kinds.get(params.value.itemId);
          // Only "agentMessage" text is ever the model's answer; tool/shell
          // output and unmapped item kinds must never masquerade as
          // assistant text downstream.
          const CONTENT_DELTA_STREAM_KIND: Record<
            string,
            "assistant_text" | "reasoning_text" | "command_output"
          > = {
            agentMessage: "assistant_text",
            reasoning: "reasoning_text",
            toolCall: "command_output",
            userShell: "command_output",
          };
          const streamKind = (kind && CONTENT_DELTA_STREAM_KIND[kind]) ?? "unknown";
          const activeTurnId = yield* Ref.get(state.activeTurnId);
          yield* emit({
            ...base,
            ...(Option.isSome(activeTurnId) ? { turnId: activeTurnId.value } : {}),
            itemId: RuntimeItemId.make(params.value.itemId),
            type: "content.delta",
            payload: {
              streamKind,
              delta: params.value.delta,
            },
          } as ProviderRuntimeEvent);
          // Count what the stream has delivered for this item so a later
          // durable re-emission only publishes the part that is missing.
          yield* noteStreamedContent(state, params.value.itemId, params.value.delta);
          return;
        }
        case "approval/requested": {
          const params = yield* decode(MspSchema.ApprovalRequestParams);
          if (Option.isNone(params)) return;
          const p = params.value;
          yield* Ref.set(state.viewCursor, p.viewCursor);
          yield* Ref.update(state.pendingApprovals, (current) =>
            new Map(current).set(p.approvalId, {
              availableChoices: p.availableChoices,
              currentRequirementId: p.currentRequirementId,
              sessionId: p.sessionId,
              turnId: p.turnId,
              itemId: p.itemId,
              toolName: p.toolName,
              rawArgs: p.rawArgs,
              subject: p.subject as Record<string, unknown>,
            }),
          );
          yield* emit({
            ...base,
            turnId: TurnId.make(p.turnId),
            itemId: RuntimeItemId.make(p.itemId),
            requestId: RuntimeRequestId.make(p.approvalId),
            type: "request.opened",
            payload: {
              requestType: "unknown",
              // Show the actual operation being authorized, not just the
              // tool name, so approval is an informed decision.
              detail: p.rawArgs.trim().length > 0 ? p.rawArgs : p.toolName,
              args: { toolName: p.toolName, rawArgs: p.rawArgs, subject: p.subject },
              options: buildApprovalOptions(p.availableChoices),
            },
          } as ProviderRuntimeEvent);
          return;
        }
        case "approval/updated": {
          const params = yield* decode(MspSchema.ApprovalUpdatedParams);
          if (Option.isNone(params)) return;
          yield* Ref.set(state.viewCursor, params.value.viewCursor);
          const updated = yield* Ref.modify(state.pendingApprovals, (current) => {
            const existing = current.get(params.value.approvalId);
            if (!existing) return [Option.none<PendingApproval>(), current] as const;
            const next: PendingApproval = {
              ...existing,
              availableChoices: params.value.availableChoices,
              currentRequirementId: params.value.currentRequirementId,
            };
            return [
              Option.some(next),
              new Map(current).set(params.value.approvalId, next),
            ] as const;
          });
          // Re-issued pending set with a fresh stage: re-emit `request.opened`
          // for the same request ID with the new choices so the client's
          // approval card offers the current, not stale, options.
          if (Option.isSome(updated)) {
            yield* emit({
              ...base,
              turnId: TurnId.make(updated.value.turnId),
              itemId: RuntimeItemId.make(updated.value.itemId),
              requestId: RuntimeRequestId.make(params.value.approvalId),
              type: "request.opened",
              payload: {
                requestType: "unknown",
                detail:
                  updated.value.rawArgs.trim().length > 0
                    ? updated.value.rawArgs
                    : updated.value.toolName,
                args: {
                  toolName: updated.value.toolName,
                  rawArgs: updated.value.rawArgs,
                  subject: updated.value.subject,
                },
                options: buildApprovalOptions(updated.value.availableChoices),
              },
            } as ProviderRuntimeEvent);
          }
          return;
        }
        case "approval/resolved": {
          const params = yield* decode(MspSchema.ApprovalResolvedParams);
          if (Option.isNone(params)) return;
          yield* Ref.set(state.viewCursor, params.value.viewCursor);
          yield* Ref.update(state.pendingApprovals, (current) => {
            const next = new Map(current);
            next.delete(params.value.approvalId);
            return next;
          });
          yield* emit({
            ...base,
            turnId: TurnId.make(params.value.turnId),
            itemId: RuntimeItemId.make(params.value.itemId),
            requestId: RuntimeRequestId.make(params.value.approvalId),
            type: "request.resolved",
            payload: { requestType: "unknown", decision: params.value.decision },
          } as ProviderRuntimeEvent);
          return;
        }
        case "userInput/requested": {
          const params = yield* decode(MspSchema.UserInputRequestParams);
          if (Option.isNone(params)) return;
          const p = params.value;
          yield* Ref.set(state.viewCursor, p.viewCursor);
          yield* Ref.update(state.pendingUserInputs, (current) =>
            new Map(current).set(p.userInputId, p),
          );
          yield* emit({
            ...base,
            turnId: TurnId.make(p.turnId),
            itemId: RuntimeItemId.make(p.itemId),
            requestId: RuntimeRequestId.make(p.userInputId),
            type: "user-input.requested",
            payload: {
              questions: p.questions.map((question) => ({
                id: question.id,
                header: question.header,
                question: question.question,
                options: question.options.map((option) => ({
                  label: option.label,
                  description: option.description ?? "",
                })),
                allowCustomAnswer: true,
                multiSelect: question.selection.mode === "multiple",
              })),
            },
          } as ProviderRuntimeEvent);
          return;
        }
        case "userInput/settled": {
          const params = yield* decode(MspSchema.UserInputSettledParams);
          if (Option.isNone(params)) return;
          yield* Ref.set(state.viewCursor, params.value.viewCursor);
          // The single place `user-input.resolved` is emitted, for both
          // paths: check the answers this adapter itself submitted first
          // (respondToUserInput stashes them here instead of emitting
          // eagerly, precisely to avoid a double resolution), falling back
          // to an externally-settled request (timeout/interruption/another
          // client) with no known answers.
          const answeredLocally = yield* Ref.modify(state.pendingAnsweredUserInputs, (current) => {
            const existing = current.get(params.value.userInputId);
            if (!existing) return [Option.none<AnsweredUserInput>(), current] as const;
            const next = new Map(current);
            next.delete(params.value.userInputId);
            return [Option.some(existing), next] as const;
          });
          if (Option.isSome(answeredLocally)) {
            yield* emit({
              ...base,
              turnId: TurnId.make(answeredLocally.value.pending.turnId),
              itemId: RuntimeItemId.make(answeredLocally.value.pending.itemId),
              requestId: RuntimeRequestId.make(params.value.userInputId),
              type: "user-input.resolved",
              payload: { answers: answeredLocally.value.answers },
            } as ProviderRuntimeEvent);
            return;
          }
          const pending = yield* Ref.modify(state.pendingUserInputs, (current) => {
            const existing = current.get(params.value.userInputId);
            if (!existing)
              return [Option.none<MspSchema.UserInputRequestParams>(), current] as const;
            const next = new Map(current);
            next.delete(params.value.userInputId);
            return [Option.some(existing), next] as const;
          });
          if (Option.isSome(pending)) {
            yield* emit({
              ...base,
              turnId: TurnId.make(pending.value.turnId),
              itemId: RuntimeItemId.make(pending.value.itemId),
              requestId: RuntimeRequestId.make(params.value.userInputId),
              type: "user-input.resolved",
              payload: { answers: {} },
            } as ProviderRuntimeEvent);
          }
          return;
        }
        case "view/gap": {
          const params = yield* decode(MspSchema.ViewGapParams);
          if (Option.isNone(params)) return;
          yield* fillViewGap(state, params.value);
          return;
        }
        default:
          // Unmapped MSP notification (session/branchChanged, session/goalChanged,
          // session/todoListChanged, session/tokenUsage, session/contextUsage,
          // turn/retracted, turn/retryScheduled, turn/unqueued, session/*Changed
          // — deferred; not silently misrepresented as something else).
          // Note there is no `subagent/*` notification in MSP v1: delegated
          // work is observable only as `subagent`/`workflow` transcript items,
          // which the item lifecycle above already projects onto tasks.
          return;
      }
    });

  /**
   * Splice-fill recovery for `view/gap` (MSP's D-030 / FR-013 sanctioned
   * recovery, the first of the two options).
   *
   * `view/gap` names a hole as the exclusive bracket `(after, next)`. The fill
   * pages that bracket forward through `view/page` and replays each recovered
   * event through the ordinary notification dispatch, so a recovered
   * `item/completed` produces exactly the same runtime events a live one
   * would.
   *
   * The "buffer live events at cursors >= next" half of the sanctioned recipe
   * needs no buffer here: the notification consumer is a single sequential
   * fiber draining an unbounded queue, so everything the host pushes while
   * this fill runs simply waits its turn behind us and is applied after the
   * spliced range. Overlap is discarded by paging exclusively — the event
   * whose cursor equals `next` was already delivered live and stops the walk
   * without being replayed.
   *
   * Two things are deliberately preserved across the fill:
   *   - `state.viewCursor` is restored afterwards. Replayed events carry older
   *     cursors, and letting them win would rewind the cursor this adapter
   *     persists as its resume position.
   *   - `item/started` for an item this thread already knows is replayed as
   *     `item/updated`. `next` may name an ephemeral-sourced event (an
   *     `item/delta`) that `view/page` never serves, in which case the walk
   *     runs to the end of the page budget and can re-cover ground the live
   *     stream already delivered; re-opening a known item would be the one
   *     visible lie in that case.
   */
  const fillViewGap = (state: MuseThreadState, gap: MspSchema.ViewGapParams): Effect.Effect<void> =>
    Effect.gen(function* () {
      const liveCursor = yield* Ref.get(state.viewCursor);
      const museSessionId = yield* Ref.get(state.museSessionId);
      // A gap names the session whose subscription dropped events. After a
      // rollback this thread is on a NEW session (MSP has no in-place rewind)
      // while a gap for the source session can still be queued behind us:
      // filling it would repopulate the replacement's item and task state and
      // move its active turn. Only our own session's gaps are ours to fill.
      if (gap.sessionId !== museSessionId) return;
      const sessionId = museSessionId;

      let cursor: string | undefined = gap.after;
      let pages = 0;
      let recovered = 0;
      let reachedNext = false;
      let failure: string | undefined;

      while (pages < VIEW_GAP_MAX_PAGES && !reachedNext) {
        // Annotated because the fill is mutually recursive with the live
        // dispatch (`replayGapEvent` -> `handleNotification` -> `fillViewGap`),
        // which leaves TypeScript no non-circular inference path here.
        const page: Option.Option<MspSchema.ViewPageResult> = yield* state.client
          .viewPage({
            sessionId,
            ...(cursor === undefined ? {} : { cursor }),
            direction: "forward",
            limit: VIEW_GAP_PAGE_LIMIT,
          })
          .pipe(
            Effect.matchEffect({
              onFailure: (error) =>
                Effect.sync(() => {
                  failure = error.message;
                  return Option.none<MspSchema.ViewPageResult>();
                }),
              onSuccess: (result) =>
                Effect.succeed(Option.some(result) as Option.Option<MspSchema.ViewPageResult>),
            }),
          );
        if (Option.isNone(page)) break;
        pages += 1;

        for (const event of page.value.events) {
          const eventCursor =
            typeof event.params.viewCursor === "string" ? event.params.viewCursor : undefined;
          // Two exclusive upper bounds, both by cursor EQUALITY so cursors stay
          // opaque:
          //
          //   1. `next` itself, which arrived live.
          //   2. any cursor this thread already dispatched.
          //
          // (2) is what makes the walk safe when `next` names an ephemeral
          // event (an `item/delta`) that `view/page` never serves and (1) can
          // therefore never match. Without it the walk runs past the hole and
          // re-dispatches events the live stream already delivered — replaying
          // a `turn/completed` there would clear `activeTurnId` while the live
          // events queued behind this fill are still waiting, stripping their
          // turn association. Pages are ascending and contiguous and the hole
          // is one contiguous bracket, so the first already-seen cursor proves
          // the walk has reached delivered territory and everything beyond it
          // was delivered too.
          if (eventCursor !== undefined) {
            if (eventCursor === gap.next) {
              reachedNext = true;
              break;
            }
            const alreadyDispatched = yield* Ref.get(state.dispatchedViewCursors);
            if (alreadyDispatched.has(eventCursor)) {
              reachedNext = true;
              break;
            }
          }
          yield* replayGapEvent(state, event);
          recovered += 1;
        }

        if (reachedNext) break;
        const nextCursor: string | null = page.value.nextCursor;
        if (nextCursor === null) break;
        cursor = nextCursor;
      }

      // Restore the live position; the replay walked backwards through
      // already-superseded cursors.
      yield* Ref.set(state.viewCursor, liveCursor);

      if (failure !== undefined) {
        const base = yield* emitBase({ threadId: state.threadId });
        yield* emit({
          ...base,
          type: "runtime.warning",
          payload: {
            message: `Muse dropped notifications (view/gap) and the gap could not be fully recovered: ${failure}. ${recovered} event(s) were recovered; some activity may be missing until the next turn.`,
          },
        } as ProviderRuntimeEvent);
        return;
      }
      if (!reachedNext && pages >= VIEW_GAP_MAX_PAGES) {
        const base = yield* emitBase({ threadId: state.threadId });
        yield* emit({
          ...base,
          type: "runtime.warning",
          payload: {
            message: `Muse dropped notifications (view/gap); ${recovered} event(s) were recovered before the recovery budget ran out, so some activity may still be missing.`,
          },
        } as ProviderRuntimeEvent);
      }
    });

  /**
   * Replays one recovered `view/page` event through the live dispatch. The
   * only rewrite is `item/started` -> `item/updated` for an item this thread
   * already knows about, so a re-covered range updates rather than re-opens.
   */
  const replayGapEvent = (
    state: MuseThreadState,
    event: MspSchema.UnframedViewNotification,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      let method = event.method;
      if (method === "item/started") {
        const params = event.params as { readonly item?: { readonly itemId?: unknown } };
        const itemId = params.item?.itemId;
        if (typeof itemId === "string") {
          const known = yield* Ref.get(state.itemKindByItemId);
          if (known.has(itemId)) method = "item/updated";
        }
      }
      yield* handleNotification(state, { method, params: event.params });
    });

  const mapMspDecisionToProvider = (decision: string): ProviderApprovalDecision => {
    switch (decision) {
      case "approved":
        return "accept";
      case "approvedForSession":
        return "acceptForSession";
      case "approvedPolicyAmendment":
        return "acceptAlways";
      case "denied":
      case "deniedPolicyAmendment":
        return "decline";
      default:
        return "cancel";
    }
  };

  /**
   * MSP can offer multiple native choices that map to the same canonical
   * `ProviderApprovalDecision` (e.g. `denied` and `deniedPolicyAmendment` both
   * mean "decline"). The client only round-trips the canonical decision, not
   * the native `choiceId`, so offering two buttons for the same decision
   * makes `resolveMuseChoice` guess which one was actually meant. Keep only
   * the first native choice per canonical decision — the same first-match
   * order `resolveMuseChoice` itself resolves with — so whichever button is
   * shown is provably the one that gets submitted.
   */
  const buildApprovalOptions = (availableChoices: ReadonlyArray<MspSchema.ApprovalChoice>) => {
    const seen = new Set<ProviderApprovalDecision>();
    const options: Array<{ decision: ProviderApprovalDecision; label: string }> = [];
    for (const choice of availableChoices) {
      const decision = mapMspDecisionToProvider(choice.decision);
      if (seen.has(decision)) continue;
      seen.add(decision);
      options.push({ decision, label: choice.label });
    }
    return options;
  };

  const closeThread = (threadId: ThreadId) =>
    Ref.modify(threads, (current) => {
      const state = current.get(threadId);
      if (!state) return [Effect.void, current] as const;
      const next = new Map(current);
      next.delete(threadId);
      return [Scope.close(state.scope, Exit.void), next] as const;
    }).pipe(Effect.flatten, Effect.ignore);

  const startSession = (
    input: ProviderSessionStartInput,
  ): Effect.Effect<ProviderSession, ProviderAdapterError> =>
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      // Nothing is reachable through `threads` until the whole startup
      // sequence below succeeds. If any step in it fails or is interrupted
      // (spawn, initialize, session/start|resume all included), close this
      // scope here so the spawned host process and its reader/writer fibers
      // don't leak — `stopAll` can only find threads already in the map.
      return yield* startSessionInScope(input, scope).pipe(
        Effect.tapCause((cause) => Scope.close(scope, Exit.failCause(cause))),
      );
    });

  const startSessionInScope = (
    input: ProviderSessionStartInput,
    scope: Scope.Closeable,
  ): Effect.Effect<ProviderSession, ProviderAdapterError> =>
    Effect.gen(function* () {
      const cwd = input.cwd;
      const parsedResumeCursor = parseMuseResumeCursor(input.resumeCursor);
      if (parsedResumeCursor === "invalid") {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue:
            "Muse resume cursor is malformed; refusing to silently start a new session over it.",
        });
      }
      const resumeCursor = parsedResumeCursor;

      const client = yield* resolveSpawnCommand(
        options.binaryPath,
        ["serve", "--trust-workspace"],
        {
          env: environment,
          extendEnv: true,
        },
      ).pipe(
        Effect.flatMap((spawnCommand) =>
          MspClient.spawn({
            command: spawnCommand.command,
            args: spawnCommand.args,
            env: environment,
            ...(cwd ? { cwd } : {}),
            shell: spawnCommand.shell,
          }),
        ),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Scope.provide(scope),
        Effect.mapError(mapMspError(input.threadId, "spawn")),
      );

      yield* client
        .initialize({ clientInfo: { name: "t3_code", version: "0.0.0" } })
        .pipe(Effect.mapError(mapMspError(input.threadId, "initialize")));

      const commandId = yield* randomUuidV7;
      const approvalMode = runtimeModeToApprovalMode(input.runtimeMode);
      const startResult = resumeCursor
        ? yield* client
            .sessionResume({
              commandId,
              sessionId: resumeCursor.museSessionId,
              cursor: resumeCursor.viewCursor ?? null,
              // Explicit, not "auto": snapshot/anchored-snapshot history
              // responses return `items: null`, which would silently skip
              // the item-kind/content seeding below and leave an in-flight
              // agentMessage's subsequent deltas unclassified.
              history: "inline",
            })
            .pipe(
              Effect.mapError(mapMspError(input.threadId, "session/resume")),
              Effect.map((result) => ({
                session: result.session,
                viewCursor: result.viewCursor,
                historyItems: result.history.items,
              })),
            )
        : yield* client
            .sessionStart({
              commandId,
              approvalMode,
              ...(cwd ? { workspaceRoot: cwd } : {}),
              ...(input.modelSelection?.model ? { modelId: input.modelSelection.model } : {}),
            })
            .pipe(
              Effect.mapError(mapMspError(input.threadId, "session/start")),
              Effect.map((result) => ({
                ...result,
                historyItems: null as ReadonlyArray<unknown> | null,
              })),
            );

      if (resumeCursor) {
        // `session/resume` doesn't accept `approvalMode`/`modelId`; apply
        // both explicitly afterward so a resumed session honors the current
        // request's runtime mode and model instead of silently keeping
        // whatever the host had configured on a previous connection.
        const setApprovalCommandId = yield* randomUuidV7;
        yield* client
          .sessionSetApprovalMode({
            commandId: setApprovalCommandId,
            mode: approvalMode,
            sessionId: startResult.session.sessionId,
          })
          .pipe(Effect.mapError(mapMspError(input.threadId, "session/setApprovalMode")));
        if (input.modelSelection?.model) {
          const setModelCommandId = yield* randomUuidV7;
          yield* client
            .sessionSetModel({
              commandId: setModelCommandId,
              model: { modelId: input.modelSelection.model },
              sessionId: startResult.session.sessionId,
            })
            .pipe(Effect.mapError(mapMspError(input.threadId, "session/setModel")));
        }
      }

      const sessionCreatedAt = yield* nowIso;
      const state: MuseThreadState = {
        threadId: input.threadId,
        scope,
        client,
        museSessionId: yield* Ref.make(startResult.session.sessionId),
        cwd,
        viewCursor: yield* Ref.make(startResult.viewCursor),
        activeTurnId: yield* Ref.make(
          startResult.session.activeTurnId
            ? Option.some(TurnId.make(startResult.session.activeTurnId))
            : Option.none<TurnId>(),
        ),
        pendingApprovals: yield* Ref.make(new Map<string, PendingApproval>()),
        pendingUserInputs: yield* Ref.make(new Map<string, MspSchema.UserInputRequestParams>()),
        pendingAnsweredUserInputs: yield* Ref.make(new Map<string, AnsweredUserInput>()),
        items: yield* Ref.make<Array<MspSchema.Item>>([]),
        itemKindByItemId: yield* Ref.make(new Map<string, string>()),
        emittedTaskIds: yield* Ref.make(new Set<string>()),
        completedTaskIds: yield* Ref.make(new Set<string>()),
        taskItemRevision: yield* Ref.make(new Map<string, number>()),
        dispatchedViewCursors: yield* Ref.make(new Set<string>()),
        streamedContentChars: yield* Ref.make(new Map<string, number>()),
        createdAt: sessionCreatedAt,
        runtimeMode: yield* Ref.make(input.runtimeMode),
        modelId: yield* Ref.make(input.modelSelection?.model),
        sessionLock: yield* Semaphore.make(1),
      };
      if (startResult.historyItems) {
        // Seed item-kind/content state from the resumed session's history
        // before consuming live notifications: without this, deltas for an
        // already-in-flight agentMessage classify as "unknown" (dropped by
        // ingestion) until another lifecycle event happens to arrive.
        const historyItemRecords = decodeHistoryItems(startResult.historyItems);
        if (historyItemRecords.length > 0) {
          yield* Ref.set(
            state.itemKindByItemId,
            new Map(historyItemRecords.map((item) => [item.itemId, item.kind] as const)),
          );
          yield* Ref.set(state.items, historyItemRecords);
        }
      }
      if (resumeCursor) {
        // Resume drops in-flight approval/user-input requests unless they're
        // explicitly reconciled: without this, a session restored while
        // waiting on one answers as "unknown request" forever. Reconciling
        // this snapshot BEFORE `attachNotificationConsumer` starts consuming
        // live notifications (below) avoids racing a concurrent
        // approval/updated or approval/resolved against this replace — any
        // notification that arrives meanwhile just queues (the incoming
        // queue is unbounded) until the consumer attaches.
        const pending = yield* client
          .approvalListPending({ sessionId: startResult.session.sessionId })
          .pipe(Effect.mapError(mapMspError(input.threadId, "approval/listPending")));
        yield* Ref.set(
          state.pendingApprovals,
          new Map(
            pending.approvals.map(
              (p) =>
                [
                  p.approvalId,
                  {
                    availableChoices: p.availableChoices,
                    currentRequirementId: p.currentRequirementId,
                    sessionId: p.sessionId,
                    turnId: p.turnId,
                    itemId: p.itemId,
                    toolName: p.toolName,
                    rawArgs: p.rawArgs,
                    subject: p.subject as Record<string, unknown>,
                  },
                ] as const,
            ),
          ),
        );
        yield* Effect.forEach(
          pending.approvals,
          (p) =>
            Effect.gen(function* () {
              // A fresh eventId/createdAt per emission: reusing one across
              // multiple restored requests would collide on the same
              // activity ID downstream and each overwrite the last.
              const restoredBase = yield* emitBase({ threadId: input.threadId });
              yield* emit({
                ...restoredBase,
                turnId: TurnId.make(p.turnId),
                itemId: RuntimeItemId.make(p.itemId),
                requestId: RuntimeRequestId.make(p.approvalId),
                type: "request.opened",
                payload: {
                  requestType: "unknown",
                  detail: p.rawArgs.trim().length > 0 ? p.rawArgs : p.toolName,
                  args: { toolName: p.toolName, rawArgs: p.rawArgs, subject: p.subject },
                  options: buildApprovalOptions(p.availableChoices),
                },
              } as ProviderRuntimeEvent);
            }),
          { discard: true },
        );
        const decodedUserInputs = yield* Effect.forEach(pending.userInputs, (raw) =>
          Schema.decodeUnknownEffect(MspSchema.UserInputRequestParams)(raw).pipe(Effect.option),
        );
        const userInputs = decodedUserInputs.filter(Option.isSome).map((decoded) => decoded.value);
        yield* Ref.set(
          state.pendingUserInputs,
          new Map(userInputs.map((p) => [p.userInputId, p] as const)),
        );
        yield* Effect.forEach(
          userInputs,
          (p) =>
            Effect.gen(function* () {
              const restoredBase = yield* emitBase({ threadId: input.threadId });
              yield* emit({
                ...restoredBase,
                turnId: TurnId.make(p.turnId),
                itemId: RuntimeItemId.make(p.itemId),
                requestId: RuntimeRequestId.make(p.userInputId),
                type: "user-input.requested",
                payload: {
                  questions: p.questions.map((question) => ({
                    id: question.id,
                    header: question.header,
                    question: question.question,
                    options: question.options.map((option) => ({
                      label: option.label,
                      description: option.description ?? "",
                    })),
                    allowCustomAnswer: true,
                    multiSelect: question.selection.mode === "multiple",
                  })),
                },
              } as ProviderRuntimeEvent);
            }),
          { discard: true },
        );
      }
      // Publish only now, after spawn/initialize/session-start-or-resume AND
      // (for resume) pending-request reconciliation have all succeeded —
      // publishing earlier and failing partway through would leave a dead
      // entry in `threads` with no consumer ever attached to remove it,
      // routing this thread at a closed client until the process restarts.
      // A reconnect for a thread that already has a live host (e.g. a resume
      // racing an existing session) must not leak the old scope/process.
      // Read-and-set as one atomic `Ref.modify`, not a separate get/update:
      // two concurrent starts for the same thread must not both observe "no
      // previous entry" and each overwrite the other's, leaking whichever
      // host loses the race from `stopAll`'s view entirely.
      const previous = yield* Ref.modify(threads, (current) => {
        const existing = current.get(input.threadId);
        return [existing, new Map(current).set(input.threadId, state)] as const;
      });
      if (previous) yield* Scope.close(previous.scope, Exit.void).pipe(Effect.ignore);
      yield* Scope.addFinalizer(
        scope,
        Effect.sync(() => undefined),
      );
      yield* attachNotificationConsumer(state);

      const base = yield* emitBase({ threadId: input.threadId });
      yield* emit({
        ...base,
        type: "session.started",
        payload: {
          resume: {
            schemaVersion: RESUME_CURSOR_VERSION,
            museSessionId: startResult.session.sessionId,
          },
        },
      } as ProviderRuntimeEvent);

      return {
        provider: PROVIDER,
        providerInstanceId: options.instanceId,
        status: startResult.session.activeTurnId ? "running" : "ready",
        ...(startResult.session.activeTurnId
          ? { activeTurnId: TurnId.make(startResult.session.activeTurnId) }
          : {}),
        runtimeMode: input.runtimeMode,
        ...(cwd ? { cwd } : {}),
        ...(input.modelSelection?.model ? { model: input.modelSelection.model } : {}),
        threadId: input.threadId,
        resumeCursor: {
          schemaVersion: RESUME_CURSOR_VERSION,
          museSessionId: startResult.session.sessionId,
        },
        createdAt: yield* nowIso,
        updatedAt: yield* nowIso,
      } satisfies ProviderSession;
    });

  const sendTurn = (
    input: ProviderSendTurnInput,
  ): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> =>
    withThreadLock(input.threadId, (state) =>
      Effect.gen(function* () {
        const museSessionId = yield* Ref.get(state.museSessionId);
        // Images ride the turn as native MSP `image` parts. Everything else
        // (files, unknown types) already reaches the model through the on-disk
        // path line ProviderService appends to the prompt, which is also MSP's
        // own documented mechanism for files ("File mentions are text, not a
        // part type"), so those need no part and no warning.
        const imageParts = yield* Effect.forEach(
          (input.attachments ?? []).filter((attachment) => attachment.type === "image"),
          (attachment) => readImageInputPart(input.threadId, attachment),
          { concurrency: 1 },
        );
        // A turn carrying only images is still a real submission; MSP requires
        // a non-empty `input` array, not non-empty text.
        if ((!input.input || input.input.trim().length === 0) && imageParts.length === 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Muse requires turn input; promptless continuation is not supported.",
          });
        }
        if (input.modelSelection?.model) {
          const currentModel = yield* Ref.get(state.modelId);
          if (currentModel !== input.modelSelection.model) {
            const setModelCommandId = yield* randomUuidV7;
            yield* state.client
              .sessionSetModel({
                commandId: setModelCommandId,
                model: { modelId: input.modelSelection.model },
                sessionId: museSessionId,
              })
              .pipe(Effect.mapError(mapMspError(input.threadId, "session/setModel")));
            yield* Ref.set(state.modelId, input.modelSelection.model);
          }
        }
        const commandId = yield* randomUuidV7;
        const result = yield* state.client
          .turnStart({
            commandId,
            input: [
              ...(input.input && input.input.trim().length > 0
                ? [{ type: "text" as const, text: input.input }]
                : []),
              ...imageParts,
            ],
            sessionId: museSessionId,
          })
          .pipe(Effect.mapError(mapMspError(input.threadId, "turn/start")));

        return {
          threadId: input.threadId,
          turnId: TurnId.make(result.turnId),
          resumeCursor: { schemaVersion: RESUME_CURSOR_VERSION, museSessionId },
        } satisfies ProviderTurnStartResult;
      }),
    );

  const interruptTurn = (
    threadId: ThreadId,
    turnId?: TurnId,
  ): Effect.Effect<void, ProviderAdapterError> =>
    withThreadLock(threadId, (state) =>
      Effect.gen(function* () {
        const commandId = yield* randomUuidV7;
        const sessionId = yield* Ref.get(state.museSessionId);
        yield* state.client
          .turnInterrupt({ commandId, sessionId, ...(turnId ? { turnId } : {}) })
          .pipe(Effect.mapError(mapMspError(threadId, "turn/interrupt")), Effect.asVoid);
      }),
    );

  const respondToRequest = (
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    decision: ProviderApprovalDecision,
  ): Effect.Effect<void, ProviderAdapterError> =>
    Effect.gen(function* () {
      const state = yield* requireThread(threadId);
      const pending = (yield* Ref.get(state.pendingApprovals)).get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "respondToRequest",
          issue: `Unknown or already-resolved Muse approval: ${requestId}`,
        });
      }
      const choice = resolveMuseChoice(decision, pending.availableChoices);
      if (!choice) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "respondToRequest",
          issue: `Muse did not offer a choice matching decision '${decision}' for approval ${requestId}.`,
        });
      }
      const commandId = yield* randomUuidV7;
      yield* state.client
        .approvalDecide({
          approvalId: requestId,
          choiceId: choice.choiceId,
          commandId,
          requirementId: pending.currentRequirementId,
          sessionId: pending.sessionId,
        })
        .pipe(Effect.mapError(mapMspError(threadId, "approval/decide")), Effect.asVoid);
    });

  const respondToUserInput = (
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    answers: ProviderUserInputAnswers,
  ): Effect.Effect<void, ProviderAdapterError> =>
    Effect.gen(function* () {
      const state = yield* requireThread(threadId);
      // Move the request from pendingUserInputs to pendingAnsweredUserInputs
      // atomically and BEFORE calling userInputAnswer: the protocol dispatches
      // notifications independently of RPC responses, so `userInput/settled`
      // can arrive before this RPC's own response. Recording the answer only
      // after the RPC returns left a window where settlement found neither
      // map and emitted nothing, or found only the empty-answers fallback.
      const pending = yield* Ref.modify(state.pendingUserInputs, (current) => {
        const existing = current.get(requestId);
        if (!existing) return [undefined, current] as const;
        const next = new Map(current);
        next.delete(requestId);
        return [existing, next] as const;
      });
      if (!pending) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "respondToUserInput",
          issue: `Unknown or already-settled Muse user-input prompt: ${requestId}`,
        });
      }
      yield* Ref.update(state.pendingAnsweredUserInputs, (current) =>
        new Map(current).set(requestId, { pending, answers }),
      );
      const commandId = yield* randomUuidV7;
      const mspAnswers: Array<MspSchema.UserInputAnswer> = pending.questions.map((question) => {
        const raw = answers[question.id];
        if (Array.isArray(raw)) return { questionId: question.id, selectedLabels: raw.map(String) };
        if (typeof raw === "string") {
          // The web client sends a single selected option as a plain string,
          // not an array — match it against the offered labels so a real
          // selection uses MSP's `selectedLabel`, reserving `freeText` for
          // genuinely custom answers.
          const matchesOption = question.options.some((option) => option.label === raw);
          return matchesOption
            ? { questionId: question.id, selectedLabel: raw }
            : { questionId: question.id, freeText: raw };
        }
        return { questionId: question.id, freeText: raw === undefined ? "" : JSON.stringify(raw) };
      });
      // Don't emit `user-input.resolved` here: the host's own
      // `userInput/settled` notification is the single source of truth for
      // when this request is actually done, and emitting eagerly here too
      // would double-resolve it (ingestion would persist two separate
      // "submitted" activities for one request, in either arrival order).
      yield* state.client
        .userInputAnswer({
          answers: mspAnswers,
          commandId,
          sessionId: pending.sessionId,
          userInputId: requestId,
        })
        .pipe(
          Effect.mapError(mapMspError(threadId, "userInput/answer")),
          Effect.asVoid,
          Effect.tapError(() =>
            // Nothing was actually sent to Muse: undo the optimistic
            // answered-state move so the request goes back to pending
            // instead of being stuck "answered" but never settled.
            Ref.update(state.pendingAnsweredUserInputs, (current) => {
              const next = new Map(current);
              next.delete(requestId);
              return next;
            }).pipe(
              Effect.andThen(
                Ref.update(state.pendingUserInputs, (current) =>
                  new Map(current).set(requestId, pending),
                ),
              ),
            ),
          ),
        );
    });

  const stopSession = (threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
    closeThread(threadId);

  const listSessions = () =>
    Ref.get(threads).pipe(
      Effect.flatMap((current) =>
        Effect.forEach(Array.from(current.values()), (state) =>
          Effect.all({
            runtimeMode: Ref.get(state.runtimeMode),
            activeTurnId: Ref.get(state.activeTurnId),
            museSessionId: Ref.get(state.museSessionId),
          }).pipe(
            Effect.map(({ runtimeMode, activeTurnId, museSessionId }): ProviderSession => ({
              provider: PROVIDER,
              providerInstanceId: options.instanceId,
              // `runStopAll`'s continue-after-update path only resumes
              // sessions reported "running" with an activeTurnId set —
              // report both truthfully, not an unconditional idle/ready.
              status: Option.isSome(activeTurnId) ? "running" : "ready",
              runtimeMode,
              ...(state.cwd ? { cwd: state.cwd } : {}),
              ...(Option.isSome(activeTurnId) ? { activeTurnId: activeTurnId.value } : {}),
              threadId: state.threadId,
              resumeCursor: {
                schemaVersion: RESUME_CURSOR_VERSION,
                museSessionId,
              },
              createdAt: state.createdAt,
              updatedAt: state.createdAt,
            })),
          ),
        ),
      ),
    );

  const hasSession = (threadId: ThreadId) =>
    Ref.get(threads).pipe(Effect.map((current) => current.has(threadId)));

  const readThread = (
    threadId: ThreadId,
  ): Effect.Effect<ProviderThreadSnapshot, ProviderAdapterError> =>
    Effect.gen(function* () {
      const state = yield* requireThread(threadId);
      const items = yield* Ref.get(state.items);
      return { threadId, turns: groupItemsByTurn(items) } satisfies ProviderThreadSnapshot;
    });

  /**
   * MSP has no in-place rewind, but `session/fork` with a `cutPoint` copies
   * the source history through a completed turn (inclusive) into a NEW
   * session — semantically exactly a rollback, the same way T3's git
   * checkpoints restore by moving to a new ref rather than rewriting one.
   * After the fork this thread's `museSessionId` points at the fork; the
   * source session is left intact on the host. The new id is surfaced
   * through a fresh `session.started` (its `resume` cursor) and the next
   * `sendTurn`'s `resumeCursor`, so a later resume lands on the fork.
   */
  const rollbackThread = (
    threadId: ThreadId,
    numTurns: number,
  ): Effect.Effect<ProviderThreadSnapshot, ProviderAdapterError> =>
    Effect.gen(function* () {
      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "numTurns must be an integer >= 1.",
        });
      }
      return yield* withThreadLock(threadId, (state) =>
        Effect.gen(function* () {
          if (Option.isSome(yield* Ref.get(state.activeTurnId))) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "rollbackThread",
              issue: "Cannot rewind while a Muse turn is running; interrupt it first.",
            });
          }
          const sourceSessionId = yield* Ref.get(state.museSessionId);

          // Ask the host for the authoritative history (not the in-memory cache,
          // which is best-effort and may miss items from before this process
          // attached) to find the ordered list of turns that actually exist.
          const read = yield* state.client
            .sessionRead({ sessionId: sourceSessionId, excludeItems: false })
            .pipe(Effect.mapError(mapMspError(threadId, "session/read")));
          const historyItems = decodeHistoryItems(read.history.items ?? []);
          const turnIds = orderedTurnIds(historyItems);
          if (turnIds.length < numTurns) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "rollbackThread",
              issue: `Cannot rewind ${numTurns} turn(s): this Muse session only has ${turnIds.length}.`,
            });
          }
          // The last turn to KEEP. `undefined` means "keep nothing": MSP's omitted
          // `cutPoint` means "all completed turns" (the opposite), so a rewind to
          // an empty conversation is a fresh `session/start` in the same
          // workspace instead of a fork. CheckpointReactor accepts turnCount=0
          // and restores the filesystem before calling this, so refusing here
          // would leave files reverted but the conversation intact.
          const keepThroughTurnId = turnIds[turnIds.length - numTurns - 1];
          const runtimeMode = yield* Ref.get(state.runtimeMode);
          const approvalMode = runtimeModeToApprovalMode(runtimeMode);
          const modelId = yield* Ref.get(state.modelId);

          // Prepare the replacement session FULLY (create it, apply approval mode
          // and model) before touching any local state. If any step fails or is
          // interrupted the thread still targets the untouched source session,
          // the caller gets an error, and a retry starts from the same place
          // instead of compounding on a half-configured fork.
          const replacement =
            keepThroughTurnId === undefined
              ? yield* state.client
                  .sessionStart({
                    commandId: yield* randomUuidV7,
                    approvalMode,
                    ...(state.cwd ? { workspaceRoot: state.cwd } : {}),
                    ...(modelId ? { modelId } : {}),
                  })
                  .pipe(
                    Effect.mapError(mapMspError(threadId, "session/start")),
                    Effect.map((result) => ({
                      sessionId: result.session.sessionId,
                      viewCursor: result.viewCursor,
                      items: [] as Array<MspSchema.Item>,
                      // session/start already took approvalMode/modelId.
                      needsSettings: false,
                    })),
                  )
              : yield* state.client
                  .sessionFork({
                    commandId: yield* randomUuidV7,
                    sessionId: sourceSessionId,
                    cutPoint: { lastTurnId: keepThroughTurnId },
                    excludeItems: false,
                  })
                  .pipe(
                    Effect.mapError(mapMspError(threadId, "session/fork")),
                    Effect.map((result) => ({
                      sessionId: result.session.sessionId,
                      viewCursor: result.viewCursor,
                      items: decodeHistoryItems(result.history.items ?? []),
                      needsSettings: true,
                    })),
                  );
          if (replacement.needsSettings) {
            yield* state.client
              .sessionSetApprovalMode({
                commandId: yield* randomUuidV7,
                sessionId: replacement.sessionId,
                mode: approvalMode,
              })
              .pipe(Effect.mapError(mapMspError(threadId, "session/setApprovalMode")));
            if (modelId) {
              yield* state.client
                .sessionSetModel({
                  commandId: yield* randomUuidV7,
                  model: { modelId },
                  sessionId: replacement.sessionId,
                })
                .pipe(Effect.mapError(mapMspError(threadId, "session/setModel")));
            }
          }

          // Commit: retarget every later call on this thread at the replacement.
          // All Ref writes, no yield points that can fail in between, inside
          // `uninterruptible` so a cancellation cannot leave half the state
          // pointing at the old session and half at the new one.
          yield* Effect.uninterruptible(
            Effect.gen(function* () {
              yield* Ref.set(state.museSessionId, replacement.sessionId);
              yield* Ref.set(state.viewCursor, replacement.viewCursor);
              yield* Ref.set(state.activeTurnId, Option.none());
              yield* Ref.set(state.pendingApprovals, new Map());
              yield* Ref.set(state.pendingUserInputs, new Map());
              yield* Ref.set(state.pendingAnsweredUserInputs, new Map());
              yield* Ref.set(state.items, replacement.items);
              yield* Ref.set(
                state.itemKindByItemId,
                new Map(replacement.items.map((item) => [item.itemId, item.kind] as const)),
              );
              // The fork is a different session with different item ids; the
              // task rows opened against the source must not suppress the
              // replacement's own start/completion events.
              yield* Ref.set(state.emittedTaskIds, new Set<string>());
              yield* Ref.set(state.completedTaskIds, new Set<string>());
              yield* Ref.set(state.taskItemRevision, new Map<string, number>());
              yield* Ref.set(state.streamedContentChars, new Map<string, number>());
              // Cursors are per-session; a replacement session's cursors are a
              // fresh sequence, so keeping the source's would let a gap fill
              // stop on a cursor that never belonged to this session.
              yield* Ref.set(state.dispatchedViewCursors, new Set<string>());
            }),
          );

          // ProviderService persists the new resume cursor right after this
          // returns (via listSessions), so a restart before the next turn resumes
          // the replacement, not the untrimmed source. This `session.started` is
          // the runtime-event marker that the underlying session was replaced.
          const base = yield* emitBase({ threadId });
          yield* emit({
            ...base,
            type: "session.started",
            payload: {
              message:
                keepThroughTurnId === undefined
                  ? "Rewound to the start by replacing the Muse session."
                  : `Rewound ${numTurns} turn(s) by forking the Muse session.`,
              resume: {
                schemaVersion: RESUME_CURSOR_VERSION,
                museSessionId: replacement.sessionId,
              },
            },
          } as ProviderRuntimeEvent);

          return {
            threadId,
            turns: groupItemsByTurn(replacement.items),
          } satisfies ProviderThreadSnapshot;
        }),
      );
    });

  const stopAll = (): Effect.Effect<void, ProviderAdapterError> =>
    Ref.get(threads).pipe(
      Effect.flatMap((current) =>
        Effect.forEach([...current.keys()], stopSession, { discard: true }),
      ),
    );

  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "in-session",
      promptlessTurnContinuation: false,
      // Via `session/fork` with a cut point; see `rollbackThread`.
      supportsConversationRollback: true,
    },
    compaction: {
      type: "native",
      start: (threadId) =>
        withThreadLock(threadId, (state) =>
          Effect.gen(function* () {
            const commandId = yield* randomUuidV7;
            const sessionId = yield* Ref.get(state.museSessionId);
            const result = yield* state.client
              .sessionCompact({ commandId, sessionId })
              .pipe(Effect.mapError(mapMspError(threadId, "session/compact")));
            const base = yield* emitBase({ threadId });
            // `ProviderService`/runtime ingestion complete a compaction request
            // off the canonical `thread.state.changed(compacted)` event, not
            // the item-lifecycle events Muse emits for the compaction item
            // itself — without this, the request just times out. A rejected or
            // skipped compaction (`result.reason` set) is surfaced instead of
            // silently discarded.
            if (result.reason) {
              // ProviderService waits for the canonical compaction-completed
              // event after `start` succeeds; a bare warning never settles
              // that wait, leaving a skipped/rejected compaction pending for
              // the full timeout. Fail instead so the request clears now.
              return yield* new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session/compact",
                detail: `Muse did not compact this session: ${result.reason}`,
              });
            }
            yield* emit({
              ...base,
              type: "thread.state.changed",
              payload: { state: "compacted" },
            } as ProviderRuntimeEvent);
          }),
        ),
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    streamEvents: Stream.fromPubSub(runtimeEvents),
  } satisfies ProviderAdapterShape<ProviderAdapterError>;
});
