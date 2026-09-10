/**
 * MuseProvider — status probe and snapshot for the Muse driver.
 *
 * The probe runs `muse --version` for install/version, then opens a short-lived
 * `muse serve` host in an empty temp dir and asks `model/list` for the live
 * catalog. `model/list` is a pure query in MSP v1 (no session, no `commandId`,
 * no durable record — tdd §3.10), so this stays within
 * `docs/internals/providers.md`: no session is started, no workspace is read,
 * no login can be triggered. The catalog resolves without credentials — auth
 * is still only observable when a real session starts.
 *
 * @module provider/Layers/MuseProvider
 */
import {
  type CustomModelSetting,
  type ModelCapabilities,
  type MuseSettings,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as FileSystem from "effect/FileSystem";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as MspClient from "effect-msp/client";
import type * as MspSchema from "effect-msp/schema";

import { makeMuseEnvironment } from "./MuseEnvironment.ts";

import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const MUSE_PRESENTATION = {
  displayName: "Muse Code",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({ optionDescriptors: [] });
const VERSION_PROBE_TIMEOUT_MS = 4_000;
const MODEL_LIST_PROBE_TIMEOUT_MS = 8_000;

function museModelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
  builtInModels: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(builtInModels, customModels ?? [], EMPTY_CAPABILITIES);
}

/**
 * Projects the MSP catalog onto `ServerProviderModel`. `isDefault` is carried
 * through so the picker lands on the catalog's own default; the catalog
 * already comes newest-first, so order is preserved.
 */
export function museModelsFromCatalog(
  entries: ReadonlyArray<MspSchema.ModelCatalogEntry>,
): ReadonlyArray<ServerProviderModel> {
  const models: ServerProviderModel[] = [];
  const seen = new Set<string>();
  for (const entry of entries) {
    const slug = entry.modelId.trim();
    if (!slug || seen.has(slug)) continue;
    seen.add(slug);
    models.push({
      slug,
      name: entry.displayLabel.trim() || slug,
      isCustom: false,
      ...(entry.isDefault ? { isDefault: true } : {}),
      capabilities: EMPTY_CAPABILITIES,
    });
  }
  return models;
}

/**
 * Opens a throwaway `muse serve` in an empty scoped temp dir and reads
 * `model/list`. No `--trust-workspace` and no `session/start`: the host has
 * nothing to read and nothing to run. Process and dir go away with the scope.
 */
const discoverMuseModelsViaModelList = (command: string, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const isolatedCwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-muse-probe-" });
    const spawnCommand = yield* resolveSpawnCommand(command, ["serve"], {
      env: environment,
      extendEnv: true,
    });
    const client = yield* MspClient.spawn({
      command: spawnCommand.command,
      args: spawnCommand.args,
      env: { ...environment, MUSE_NO_AUTO_UPDATE: "1" },
      cwd: isolatedCwd,
      shell: spawnCommand.shell,
    });
    yield* client.initialize({ clientInfo: { name: "t3_code_probe", version: "0.0.0" } });
    const result = yield* client.modelList({});
    return museModelsFromCatalog(result.models);
  }).pipe(Effect.scoped);

export function buildInitialMuseProviderSnapshot(
  museSettings: MuseSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = museModelsFromSettings(museSettings.customModels);
    return buildServerProvider({
      presentation: MUSE_PRESENTATION,
      enabled: museSettings.enabled,
      checkedAt,
      models,
      probe: museSettings.enabled
        ? {
            installed: true,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Checking Muse Code CLI availability...",
          }
        : {
            installed: false,
            version: null,
            status: "warning",
            auth: { status: "unknown" },
            message: "Muse Code is disabled in T3 Code settings.",
          },
    });
  });
}

export const checkMuseProviderStatus = Effect.fn("checkMuseProviderStatus")(function* (
  museSettings: MuseSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  // Custom models only, until the catalog probe below succeeds.
  const models = museModelsFromSettings(museSettings.customModels);

  if (!museSettings.enabled) {
    return buildServerProvider({
      presentation: MUSE_PRESENTATION,
      enabled: false,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Muse Code is disabled in T3 Code settings.",
      },
    });
  }

  const command = museSettings.binaryPath || "muse";
  // Probe the account this instance actually runs as, not the machine default.
  const museEnvironment = makeMuseEnvironment(environment, museSettings.homePath);
  const versionResult = yield* resolveSpawnCommand(command, ["--version"], {
    env: museEnvironment,
  }).pipe(
    Effect.flatMap((spawnCommand) =>
      spawnAndCollect(
        command,
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          env: { ...museEnvironment, MUSE_NO_AUTO_UPDATE: "1" },
          shell: spawnCommand.shell,
        }),
      ),
    ),
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    return buildServerProvider({
      presentation: MUSE_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Muse Code launcher (`muse`) is not installed or not on PATH."
          : "Failed to execute Muse Code CLI health check.",
      },
    });
  }

  if (Option.isNone(versionResult.success)) {
    return buildServerProvider({
      presentation: MUSE_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Muse Code CLI health check timed out.",
      },
    });
  }

  const { code, stdout, stderr } = versionResult.success.value;
  if (code !== 0) {
    return buildServerProvider({
      presentation: MUSE_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: `Muse Code CLI exited with code ${code}: ${stderr.trim() || stdout.trim() || "no output"}`,
      },
    });
  }

  const version = parseGenericCliVersion(stdout);

  const catalogExit = yield* discoverMuseModelsViaModelList(command, museEnvironment).pipe(
    Effect.timeoutOption(MODEL_LIST_PROBE_TIMEOUT_MS),
    Effect.exit,
  );
  const catalogModels = Exit.isSuccess(catalogExit)
    ? Option.getOrElse(catalogExit.value, () => [])
    : [];
  const catalogFailed = Exit.isFailure(catalogExit) || Option.isNone(catalogExit.value);
  if (catalogFailed) {
    yield* Effect.logWarning("Muse model/list probe failed or timed out.", {
      errorTag: Exit.isFailure(catalogExit) ? causeErrorTag(catalogExit.cause) : "Timeout",
    });
  }

  return buildServerProvider({
    presentation: MUSE_PRESENTATION,
    enabled: true,
    checkedAt,
    models: museModelsFromSettings(museSettings.customModels, catalogModels),
    probe: {
      installed: true,
      version,
      // An empty catalog is a supported Muse configuration (tdd §3.10), so
      // it is not an error on its own; a failed probe is worth surfacing.
      status: catalogFailed ? "warning" : "ready",
      // Whether the local `muse` install is signed in is only observable by
      // opening a real MSP session (`session/start` + a turn) — deferred to
      // avoid making a background health check start a session, per
      // docs/internals/providers.md.
      auth: { status: "unknown" },
      message: catalogFailed
        ? "Muse Code CLI is installed, but the model catalog could not be read. Only custom models are available until the next check."
        : "Muse Code CLI is installed. Sign-in status is checked when a session starts.",
    },
  });
});
