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
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

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
const isTextGenerationError = Schema.is(TextGenerationError);

export const makeMuseTextGeneration = Effect.fn("makeMuseTextGeneration")(function* (
  museSettings: MuseSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const runMuseJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const command = museSettings.binaryPath || "muse";
      const spawnCommand = yield* resolveSpawnCommand(command, ["serve", "--trust-workspace"], {
        env: environment,
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
        env: environment,
        cwd,
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
          workspaceRoot: cwd,
          ...(modelSelection.model ? { modelId: modelSelection.model } : {}),
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({ operation, detail: "Muse session/start failed.", cause }),
          ),
        );

      const textFiber = yield* Effect.forkChild(
        collectAgentText(client, started.session.sessionId),
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

      const trimmed = (yield* Fiber.join(textFiber).pipe(
        Effect.timeoutOption(MUSE_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                new TextGenerationError({ operation, detail: "Muse request timed out." }),
              ),
            onSome: Effect.succeed,
          }),
        ),
      )).trim();

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
 * `item/completed`) for `agentMessage` items into one string, resolving as
 * soon as `turn/completed` arrives on this session. Single-turn,
 * single-session use only — not a general-purpose transcript reader.
 */
function collectAgentText(client: MspClient.MspClient, sessionId: string): Effect.Effect<string> {
  return Ref.make("").pipe(
    Effect.flatMap((textRef) =>
      client.notifications.pipe(
        Stream.mapEffect((notification) =>
          Effect.gen(function* () {
            const params = notification.params as
              | (Record<string, unknown> & { sessionId?: unknown })
              | undefined;
            if (!params || params.sessionId !== sessionId) return false;
            if (notification.method === "item/delta") {
              const delta = params as { delta?: unknown; field?: unknown };
              if (
                typeof delta.delta === "string" &&
                (delta.field === undefined || delta.field === "text")
              ) {
                yield* Ref.update(textRef, (current) => current + delta.delta);
              }
              return false;
            }
            if (notification.method === "item/completed") {
              const item = (params as { item?: Record<string, unknown> }).item;
              if (item?.kind === "agentMessage" && typeof item.fallbackText === "string") {
                yield* Ref.update(textRef, (current) =>
                  current.length > 0 ? current : String(item.fallbackText),
                );
              }
              return false;
            }
            return notification.method === "turn/completed";
          }),
        ),
        Stream.takeUntil((done) => done),
        Stream.runDrain,
        Effect.andThen(Ref.get(textRef)),
      ),
    ),
  );
}
