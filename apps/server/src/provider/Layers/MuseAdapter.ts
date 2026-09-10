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
 * for the native session log.
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
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  type ProviderTurnStartResult,
  TurnId,
} from "@t3tools/contracts";
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

function parseMuseResumeCursor(raw: unknown): MuseResumeCursor | undefined {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return undefined;
  const record = raw as Record<string, unknown>;
  if (record.schemaVersion !== RESUME_CURSOR_VERSION) return undefined;
  if (typeof record.museSessionId !== "string" || record.museSessionId.trim().length === 0) {
    return undefined;
  }
  return {
    schemaVersion: RESUME_CURSOR_VERSION,
    museSessionId: record.museSessionId,
    ...(typeof record.viewCursor === "string" ? { viewCursor: record.viewCursor } : {}),
  };
}

interface PendingApproval {
  readonly availableChoices: ReadonlyArray<MspSchema.ApprovalChoice>;
  readonly currentRequirementId: MspSchema.ApprovalRequirementRef;
  readonly sessionId: string;
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
  /** Best-effort item cache for `readThread`; not the native session log. */
  readonly items: Ref.Ref<Array<MspSchema.Item>>;
  readonly itemKindByItemId: Ref.Ref<Map<string, string>>;
  readonly createdAt: string;
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

const itemTitle = (item: MspSchema.Item): string | undefined =>
  typeof item.toolName === "string"
    ? item.toolName
    : typeof item.commandText === "string"
      ? item.commandText
      : undefined;

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
        Effect.gen(function* () {
          const base = yield* emitBase({ threadId: state.threadId });
          yield* emit({
            ...base,
            type: "session.exited",
            payload: { reason: "host connection closed", recoverable: false, exitKind: "error" },
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
          const eventType =
            notification.method === "item/started"
              ? "item.started"
              : notification.method === "item/updated"
                ? "item.updated"
                : "item.completed";
          yield* emit({
            ...base,
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
          yield* emit({
            ...base,
            itemId: RuntimeItemId.make(params.value.itemId),
            type: "content.delta",
            payload: {
              streamKind: kind === "reasoning" ? "reasoning_text" : "assistant_text",
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
              detail: p.toolName,
              options: p.availableChoices.map((choice) => ({
                decision: mapMspDecisionToProvider(choice.decision),
                label: choice.label,
              })),
            },
          } as ProviderRuntimeEvent);
          return;
        }
        case "approval/updated": {
          // Re-issued pending set with a fresh stage; refresh what respondToRequest reads.
          const params = yield* decode(MspSchema.ApprovalUpdatedParams);
          if (Option.isNone(params)) return;
          yield* Ref.set(state.viewCursor, params.value.viewCursor);
          yield* Ref.update(state.pendingApprovals, (current) => {
            const existing = current.get(params.value.approvalId);
            if (!existing) return current;
            return new Map(current).set(params.value.approvalId, {
              ...existing,
              availableChoices: params.value.availableChoices,
              currentRequirementId: params.value.currentRequirementId,
            });
          });
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
          const raw = notification.params as
            | { userInputId?: unknown; viewCursor?: unknown }
            | undefined;
          if (raw && typeof raw.viewCursor === "string") {
            yield* Ref.set(state.viewCursor, raw.viewCursor);
          }
          if (raw && typeof raw.userInputId === "string") {
            yield* Ref.update(state.pendingUserInputs, (current) => {
              const next = new Map(current);
              next.delete(raw.userInputId as string);
              return next;
            });
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
      const cwd = input.cwd;
      const resumeCursor = parseMuseResumeCursor(input.resumeCursor);

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
      const startResult = resumeCursor
        ? yield* client
            .sessionResume({
              commandId,
              sessionId: resumeCursor.museSessionId,
              cursor: resumeCursor.viewCursor ?? null,
            })
            .pipe(
              Effect.mapError(mapMspError(input.threadId, "session/resume")),
              Effect.map((result) => ({ session: result.session, viewCursor: result.viewCursor })),
            )
        : yield* client
            .sessionStart({
              commandId,
              ...(cwd ? { workspaceRoot: cwd } : {}),
              ...(input.modelSelection?.model ? { modelId: input.modelSelection.model } : {}),
            })
            .pipe(Effect.mapError(mapMspError(input.threadId, "session/start")));

      const sessionCreatedAt = yield* nowIso;
      const state: MuseThreadState = {
        threadId: input.threadId,
        scope,
        client,
        museSessionId: startResult.session.sessionId,
        cwd,
        viewCursor: yield* Ref.make(startResult.viewCursor),
        activeTurnId: yield* Ref.make(Option.none<TurnId>()),
        pendingApprovals: yield* Ref.make(new Map<string, PendingApproval>()),
        pendingUserInputs: yield* Ref.make(new Map<string, MspSchema.UserInputRequestParams>()),
        items: yield* Ref.make<Array<MspSchema.Item>>([]),
        itemKindByItemId: yield* Ref.make(new Map<string, string>()),
        createdAt: sessionCreatedAt,
      };
      yield* Ref.update(threads, (current) => new Map(current).set(input.threadId, state));
      yield* attachNotificationConsumer(state);
      yield* Scope.addFinalizer(
        scope,
        Effect.sync(() => undefined),
      );

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
        status: "ready",
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
        if (typeof raw === "string") return { questionId: question.id, freeText: raw };
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

      const base = yield* emitBase({ threadId });
      yield* emit({
        ...base,
        requestId: RuntimeRequestId.make(requestId),
        type: "user-input.resolved",
        payload: { answers },
      } as ProviderRuntimeEvent);
    });

  const stopSession = (threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
    closeThread(threadId);

  const listSessions = () =>
    Ref.get(threads).pipe(
      Effect.map((current) =>
        Array.from(current.values()).map((state): ProviderSession => ({
          provider: PROVIDER,
          providerInstanceId: options.instanceId,
          status: "ready",
          runtimeMode: "full-access",
          ...(state.cwd ? { cwd: state.cwd } : {}),
          threadId: state.threadId,
          resumeCursor: {
            schemaVersion: RESUME_CURSOR_VERSION,
            museSessionId: state.museSessionId,
          },
          createdAt: state.createdAt,
          updatedAt: state.createdAt,
        })),
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
          yield* state.client
            .sessionCompact({ commandId, sessionId: state.museSessionId })
            .pipe(Effect.mapError(mapMspError(threadId, "session/compact")), Effect.asVoid);
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
