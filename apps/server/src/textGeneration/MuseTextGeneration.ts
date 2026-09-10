/**
 * MuseTextGeneration — commit message / PR / branch-name / thread-title
 * generation via a short-lived MSP session (`muse serve`, one `turn/start`,
 * then shutdown). Independent of `MuseAdapter`'s long-lived per-thread
 * sessions: these are one-shot, throwaway sessions scoped to the git
 * repository being summarized, not a T3 thread.
 *
 * @module textGeneration/MuseTextGeneration
 */
import { type ModelSelection, type MuseSettings, TextGenerationError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Fiber from "effect/Fiber";
import * as Deferred from "effect/Deferred";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { makeMuseEnvironment } from "../provider/Layers/MuseEnvironment.ts";

import * as MspClient from "effect-msp/client";
import { randomUuidV7 } from "effect-msp/uuid";

import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";

const MUSE_TIMEOUT_MS = 180_000;

/**
 * A configured executable that only makes sense relative to some directory:
 * `./bin/muse`, `../tools/muse`, `bin/muse`. Bare names (`muse`) are PATH
 * lookups and absolute paths (`/opt/muse`, `C:\\muse.exe`) are already
 * anchored, so neither is "relative" for this purpose.
 */
export const isRelativeExecutablePath = (command: string): boolean => {
  if (command.length === 0) return false;
  const hasSeparator = command.includes("/") || command.includes("\\");
  if (!hasSeparator) return false;
  const isPosixAbsolute = command.startsWith("/");
  const isWindowsAbsolute = /^[A-Za-z]:[\\/]/.test(command) || command.startsWith("\\\\");
  return !isPosixAbsolute && !isWindowsAbsolute;
};
const isTextGenerationError = Schema.is(TextGenerationError);
type MuseTextGenerationOperation =
  | "generateCommitMessage"
  | "generatePrContent"
  | "generateBranchName"
  | "generateThreadTitle";

export const makeMuseTextGeneration = Effect.fn("makeMuseTextGeneration")(function* (
  museSettings: MuseSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  // Text generation runs against the same Muse account as the rest of this
  // instance, so it must not silently fall back to the machine default.
  const museEnvironment = makeMuseEnvironment(environment, museSettings.homePath);

  const runMuseJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation: MuseTextGenerationOperation;
    /** The repository. The child never runs in it (see isolatedCwd); it is only
     * the base a relative `binaryPath` (e.g. `./bin/muse`) resolves against. */
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      // A relative `binaryPath` was meaningful against the repository cwd the
      // interactive adapter spawns in; anchor it there explicitly, because the
      // child below runs in an empty temp dir where `./bin/muse` resolves to
      // nothing. Bare names (`muse`) stay as-is for PATH lookup.
      const configured = museSettings.binaryPath || "muse";
      const command = isRelativeExecutablePath(configured)
        ? path.resolve(cwd, configured)
        : configured;
      // Process-level isolation, the strongest mitigation MSP v1 leaves open:
      // `muse serve` runs in an empty scoped temp dir, NOT the user's repo,
      // so even if a tool call slipped past `denyUnmatched` (below) it has
      // nothing of the user's to read or modify. The prompt built above
      // already carries every fact the model needs (diff/messages/title), and
      // this session never reads the repository itself. `cwd` (the repo) is
      // deliberately unused for the child process. The dir is removed with
      // the scope. Same approach as AntigravityTextGeneration.
      const isolatedCwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-muse-text-" }).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: "Failed to create an isolated working directory for Muse.",
              cause,
            }),
        ),
      );
      // Deliberately no `--trust-workspace`: with an untrusted (and empty)
      // workspace plus `approvalMode: "denyUnmatched"` below, this is the
      // tightest combination MSP v1 exposes for a one-shot generation session.
      const spawnCommand = yield* resolveSpawnCommand(command, ["serve"], {
        env: museEnvironment,
        extendEnv: true,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: "Failed to resolve the Muse binary.",
              cause,
            }),
        ),
      );

      const client = yield* MspClient.spawn({
        command: spawnCommand.command,
        args: spawnCommand.args,
        env: museEnvironment,
        cwd: isolatedCwd,
        shell: spawnCommand.shell,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.mapError(
          (cause) => new TextGenerationError({ operation, detail: "Failed to start Muse.", cause }),
        ),
      );

      yield* client
        .initialize({ clientInfo: { name: "t3_code_git_text", version: "0.0.0" } })
        .pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({ operation, detail: "Muse initialize failed.", cause }),
          ),
        );

      const commandId = yield* randomUuidV7;
      const started = yield* client
        .sessionStart({
          commandId,
          workspaceRoot: isolatedCwd,
          // One-shot metadata generation (commit message/PR body/branch/title)
          // must never execute workspace tools: there's no approval handler
          // attached to this session, and diff/message content is untrusted
          // input an embedded instruction could target. Combined with the
          // isolated empty cwd and no `--trust-workspace` above, `denyUnmatched`
          // auto-denies anything requiring approval instead of hanging or
          // silently running under a permissive local Muse config. MSP v1's
          // `ApprovalMode` is closed ("select, never create"), so there is no
          // client-side way to declare a no-tools policy on the wire; the
          // isolated cwd is what actually bounds the blast radius.
          approvalMode: "denyUnmatched",
          ...(modelSelection.model ? { modelId: modelSelection.model } : {}),
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({ operation, detail: "Muse session/start failed.", cause }),
          ),
        );

      const textFiber = yield* Effect.forkChild(
        collectAgentText(client, started.session.sessionId, operation),
      );

      const turnCommandId = yield* randomUuidV7;
      yield* client
        .turnStart({
          commandId: turnCommandId,
          input: [{ type: "text", text: prompt }],
          sessionId: started.session.sessionId,
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({ operation, detail: "Muse turn/start failed.", cause }),
          ),
        );

      const trimmed = (yield* Fiber.join(textFiber)).trim();

      if (!trimmed) {
        return yield* new TextGenerationError({ operation, detail: "Muse returned empty output." });
      }

      const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson));
      return yield* decodeOutput(extractJsonObject(trimmed)).pipe(
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(
              new TextGenerationError({
                operation,
                detail: "Muse returned invalid structured output.",
                cause,
              }),
            ),
        }),
      );
    }).pipe(
      // Bounds the whole spawn/initialize/session/turn/collect sequence, not
      // just the final fiber join — MSP requests await an unbounded Deferred
      // with no built-in deadline, so a live-but-nonresponsive host could
      // otherwise hang indefinitely before ever reaching a per-step timeout.
      // Wrapping the scoped Effect also closes the spawned child process on
      // timeout, since the timeout interrupts the source effect.
      Effect.timeoutOrElse({
        duration: `${MUSE_TIMEOUT_MS} millis`,
        orElse: () =>
          Effect.fail(new TextGenerationError({ operation, detail: "Muse request timed out." })),
      }),
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : new TextGenerationError({ operation, detail: "Muse text generation failed.", cause }),
      ),
      Effect.scoped,
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("MuseTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });
      const generated = yield* runMuseJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("MuseTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });
      const generated = yield* runMuseJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return { title: sanitizePrTitle(generated.title), body: generated.body.trim() };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("MuseTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });
      const generated = yield* runMuseJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return { branch: sanitizeBranchFragment(generated.branch) };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("MuseTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });
      const generated = yield* runMuseJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});

/**
 * Folds `item/delta` (and, for items that never stream deltas, the final
 * `item/completed`) into one string, resolving as soon as `turn/completed`
 * arrives on this session. Only text belonging to `agentMessage` items is
 * ever included — reasoning and tool/shell output must never leak into the
 * JSON payload this text is later decoded as — and a `turn/completed` whose
 * `terminal` isn't `"completed"` fails instead of returning whatever
 * partial JSON happened to arrive before the turn failed or was cancelled.
 * Single-turn, single-session use only — not a general-purpose transcript
 * reader.
 */
function collectAgentText(
  client: MspClient.MspClient,
  sessionId: string,
  operation: MuseTextGenerationOperation,
): Effect.Effect<string, TextGenerationError> {
  return Effect.gen(function* () {
    const kindByItemId = yield* Ref.make(new Map<string, string>());
    const textByItemId = yield* Ref.make(new Map<string, string>());
    const done = yield* Deferred.make<string, TextGenerationError>();

    yield* client.notifications.pipe(
      Stream.runForEach((notification) =>
        Effect.gen(function* () {
          const params = notification.params as
            | (Record<string, unknown> & { sessionId?: unknown })
            | undefined;
          if (!params || params.sessionId !== sessionId) return;

          if (
            notification.method === "item/started" ||
            notification.method === "item/updated" ||
            notification.method === "item/completed"
          ) {
            const item = (params as { item?: Record<string, unknown> }).item;
            if (!item || typeof item.itemId !== "string" || typeof item.kind !== "string") return;
            yield* Ref.update(kindByItemId, (current) =>
              new Map(current).set(item.itemId as string, item.kind as string),
            );
            if (
              notification.method === "item/completed" &&
              item.kind === "agentMessage" &&
              typeof item.fallbackText === "string"
            ) {
              yield* Ref.update(textByItemId, (current) => {
                const existing = current.get(item.itemId as string);
                if (existing && existing.length > 0) return current;
                return new Map(current).set(item.itemId as string, String(item.fallbackText));
              });
            }
            return;
          }

          if (notification.method === "item/delta") {
            const delta = params as { delta?: unknown; field?: unknown; itemId?: unknown };
            if (typeof delta.itemId !== "string" || typeof delta.delta !== "string") return;
            if (delta.field !== undefined && delta.field !== "text") return;
            const kinds = yield* Ref.get(kindByItemId);
            // Deltas can arrive before this item's own `item/started`; only
            // agentMessage text is ever this call's answer.
            if (kinds.get(delta.itemId) !== "agentMessage") return;
            yield* Ref.update(textByItemId, (current) =>
              new Map(current).set(
                delta.itemId as string,
                (current.get(delta.itemId as string) ?? "") + delta.delta,
              ),
            );
            return;
          }

          if (notification.method === "turn/completed") {
            const terminal = (params as { terminal?: unknown }).terminal;
            if (terminal !== "completed") {
              const errorMessage = (params as { error?: { message?: unknown } }).error?.message;
              yield* Deferred.fail(
                done,
                new TextGenerationError({
                  operation,
                  detail:
                    typeof errorMessage === "string"
                      ? `Muse turn ${String(terminal)}: ${errorMessage}`
                      : `Muse turn ended without completing (terminal: ${String(terminal)}).`,
                }),
              );
              return;
            }
            const textByItem = yield* Ref.get(textByItemId);
            yield* Deferred.succeed(done, Array.from(textByItem.values()).join(""));
          }
        }),
      ),
      Effect.catchCause((cause) =>
        Deferred.fail(
          done,
          new TextGenerationError({
            operation,
            detail: "Muse connection closed before the response completed.",
            cause,
          }),
        ),
      ),
      Effect.forkChild,
    );

    return yield* Deferred.await(done);
  });
}
