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
 * Deliberately out of scope for this pass (tracked as follow-ups, not
 * silently dropped): image/file attachments (turn input is text-only for
 * now — attachments produce a `runtime.warning` event instead of being
 * silently ignored), subagent/workflow items beyond a generic fallback
 * rendering, `view/gap` recovery (a dropped-notification bracket surfaces
 * as a `runtime.warning` today rather than a splice-fill), and
 * `readThread`, which serves this adapter's own in-memory item cache
 * (populated from live notifications) rather than a fresh `session/read`
 * query — sufficient for feedback upload and diagnostics, not a substitute
 * for the native session log. Also known and accepted: if the notification
 * queue fails on a protocol-level parse/write error (as opposed to the host
 * process actually exiting), this thread's state is dropped from the map so
 * new calls fail cleanly, but the underlying scope is not force-closed from
 * inside the consumer's own fiber — `Scope.close` on a scope from one of its
 * own children risks a self-deadlock this codebase hasn't verified is safe.
 * The common case (host process exited) doesn't hit this: the OS process is
 * already gone.
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
import * as Scope from "effect/Scope";
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

const PROVIDER = ProviderDriverKind.make("muse");
const RESUME_CURSOR_VERSION = 1 as const;

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
  readonly museSessionId: string;
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
  readonly createdAt: string;
  readonly runtimeMode: Ref.Ref<RuntimeMode>;
  readonly modelId: Ref.Ref<string | undefined>;
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
    case "toolCall":
    case "subagent":
    case "workflow":
    case "reminderChild":
      return "dynamic_tool_call";
    default:
      return "unknown";
  }
};

const itemTitle = (item: MspSchema.Item): string | undefined => {
  if (typeof item.toolName === "string") return item.toolName;
  if (typeof item.commandText === "string") return item.commandText;
  return undefined;
};

const itemDetail = (item: MspSchema.Item): string | undefined =>
  typeof item.fallbackText === "string" ? item.fallbackText : undefined;

export interface MuseAdapterOptions {
  readonly binaryPath: string;
  readonly customModels: MuseSettings["customModels"];
  readonly environment: NodeJS.ProcessEnv;
  readonly instanceId: ProviderInstanceId;
}

export const makeMuseAdapter = Effect.fn("makeMuseAdapter")(function* (
  options: MuseAdapterOptions,
): Effect.fn.Return<
  ProviderAdapterShape<ProviderAdapterError>,
  never,
  ChildProcessSpawner.ChildProcessSpawner
> {
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
            }),
      ),
      Effect.forkIn(state.scope),
    );

  const handleNotification = (
    state: MuseThreadState,
    notification: MspClient.MspNotification,
  ): Effect.Effect<void> =>
    Effect.gen(function* () {
      const base = yield* emitBase({ threadId: state.threadId });
      const decode = <A, I>(schema: Schema.Codec<A, I>) =>
        Schema.decodeUnknownEffect(schema)(notification.params).pipe(Effect.option);

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
              options: p.availableChoices.map((choice) => ({
                decision: mapMspDecisionToProvider(choice.decision),
                label: choice.label,
              })),
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
                options: updated.value.availableChoices.map((choice) => ({
                  decision: mapMspDecisionToProvider(choice.decision),
                  label: choice.label,
                })),
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
          yield* emit({
            ...base,
            type: "runtime.warning",
            payload: {
              message:
                "Muse dropped one or more notifications on this connection (view/gap); some activity may be missing until the next turn.",
            },
          } as ProviderRuntimeEvent);
          return;
        }
        default:
          // Unmapped MSP notification (session/branchChanged, session/goalChanged,
          // session/todoListChanged, session/tokenUsage, session/contextUsage,
          // turn/retracted, turn/retryScheduled, turn/unqueued, session/*Changed,
          // subagent/* — deferred; not silently misrepresented as something else).
          return;
      }
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
          env: options.environment,
          extendEnv: true,
        },
      ).pipe(
        Effect.flatMap((spawnCommand) =>
          MspClient.spawn({
            command: spawnCommand.command,
            args: spawnCommand.args,
            env: options.environment,
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
        museSessionId: startResult.session.sessionId,
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
        createdAt: sessionCreatedAt,
        runtimeMode: yield* Ref.make(input.runtimeMode),
        modelId: yield* Ref.make(input.modelSelection?.model),
      };
      if (startResult.historyItems) {
        // Seed item-kind/content state from the resumed session's history
        // before consuming live notifications: without this, deltas for an
        // already-in-flight agentMessage classify as "unknown" (dropped by
        // ingestion) until another lifecycle event happens to arrive.
        const kindEntries: Array<readonly [string, string]> = [];
        const historyItemRecords: Array<MspSchema.Item> = [];
        for (const raw of startResult.historyItems) {
          if (typeof raw !== "object" || raw === null) continue;
          const record = raw as Record<string, unknown>;
          if (typeof record.itemId !== "string" || typeof record.kind !== "string") continue;
          kindEntries.push([record.itemId, record.kind]);
          historyItemRecords.push(record as MspSchema.Item);
        }
        if (kindEntries.length > 0) {
          yield* Ref.set(state.itemKindByItemId, new Map(kindEntries));
          yield* Ref.set(state.items, historyItemRecords);
        }
      }
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
          .approvalListPending({ sessionId: state.museSessionId })
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
                  options: p.availableChoices.map((choice) => ({
                    decision: mapMspDecisionToProvider(choice.decision),
                    label: choice.label,
                  })),
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
      yield* attachNotificationConsumer(state);

      const base = yield* emitBase({ threadId: input.threadId });
      yield* emit({
        ...base,
        type: "session.started",
        payload: {
          resume: { schemaVersion: RESUME_CURSOR_VERSION, museSessionId: state.museSessionId },
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
          museSessionId: state.museSessionId,
        },
        createdAt: yield* nowIso,
        updatedAt: yield* nowIso,
      } satisfies ProviderSession;
    });

  const sendTurn = (
    input: ProviderSendTurnInput,
  ): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> =>
    Effect.gen(function* () {
      const state = yield* requireThread(input.threadId);
      if (input.attachments && input.attachments.length > 0) {
        const base = yield* emitBase({ threadId: input.threadId });
        yield* emit({
          ...base,
          type: "runtime.warning",
          payload: {
            message: `Muse turns are text-only in this integration; ${input.attachments.length} attachment(s) were not sent.`,
          },
        } as ProviderRuntimeEvent);
      }
      if (!input.input || input.input.trim().length === 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Muse requires non-empty turn input; promptless continuation is not supported.",
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
              sessionId: state.museSessionId,
            })
            .pipe(Effect.mapError(mapMspError(input.threadId, "session/setModel")));
          yield* Ref.set(state.modelId, input.modelSelection.model);
        }
      }
      const commandId = yield* randomUuidV7;
      const result = yield* state.client
        .turnStart({
          commandId,
          input: [{ type: "text", text: input.input }],
          sessionId: state.museSessionId,
        })
        .pipe(Effect.mapError(mapMspError(input.threadId, "turn/start")));

      return {
        threadId: input.threadId,
        turnId: TurnId.make(result.turnId),
        resumeCursor: { schemaVersion: RESUME_CURSOR_VERSION, museSessionId: state.museSessionId },
      } satisfies ProviderTurnStartResult;
    });

  const interruptTurn = (
    threadId: ThreadId,
    turnId?: TurnId,
  ): Effect.Effect<void, ProviderAdapterError> =>
    Effect.gen(function* () {
      const state = yield* requireThread(threadId);
      const commandId = yield* randomUuidV7;
      yield* state.client
        .turnInterrupt({ commandId, sessionId: state.museSessionId, ...(turnId ? { turnId } : {}) })
        .pipe(Effect.mapError(mapMspError(threadId, "turn/interrupt")), Effect.asVoid);
    });

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
      const pending = (yield* Ref.get(state.pendingUserInputs)).get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "respondToUserInput",
          issue: `Unknown or already-settled Muse user-input prompt: ${requestId}`,
        });
      }
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
      yield* state.client
        .userInputAnswer({
          answers: mspAnswers,
          commandId,
          sessionId: pending.sessionId,
          userInputId: requestId,
        })
        .pipe(Effect.mapError(mapMspError(threadId, "userInput/answer")), Effect.asVoid);

      // Don't emit `user-input.resolved` here: the host's own
      // `userInput/settled` notification is the single source of truth for
      // when this request is actually done, and emitting eagerly here too
      // would double-resolve it (ingestion would persist two separate
      // "submitted" activities for one request, in either arrival order).
      yield* Ref.update(state.pendingUserInputs, (current) => {
        const next = new Map(current);
        next.delete(requestId);
        return next;
      });
      yield* Ref.update(state.pendingAnsweredUserInputs, (current) =>
        new Map(current).set(requestId, { pending, answers }),
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
          }).pipe(
            Effect.map(({ runtimeMode, activeTurnId }): ProviderSession => ({
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
                museSessionId: state.museSessionId,
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
      return {
        threadId,
        // Best-effort: one synthetic turn holding every cached item, since
        // this cache does not track turn boundaries. See module docs.
        turns: [{ id: TurnId.make("muse-thread-snapshot"), items }],
      } satisfies ProviderThreadSnapshot;
    });

  const rollbackThread = (
    _threadId: ThreadId,
    _numTurns: number,
  ): Effect.Effect<ProviderThreadSnapshot, ProviderAdapterError> =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "rollbackThread",
        detail: "Muse does not expose conversation rollback over MSP v1.",
      }),
    );

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
      supportsConversationRollback: false,
    },
    compaction: {
      type: "native",
      start: (threadId) =>
        Effect.gen(function* () {
          const state = yield* requireThread(threadId);
          const commandId = yield* randomUuidV7;
          const result = yield* state.client
            .sessionCompact({ commandId, sessionId: state.museSessionId })
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
