/**
 * MuseProvider — status probe and snapshot for the Muse driver.
 *
 * Deliberately shallow for this pass: it checks that `muse --version`
 * resolves (mirrors the Grok/OpenCode CLI health check), but does not open
 * an MSP connection to fetch the live model catalog or auth state, since
 * that requires a workspace and would start a real session as a side
 * effect of a background health check — exactly what
 * `docs/internals/providers.md` warns against. Models come from
 * `customModels` settings only until a lighter-weight probe exists.
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
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

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

function museModelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings([], customModels ?? [], EMPTY_CAPABILITIES);
}

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
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
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
  const versionResult = yield* resolveSpawnCommand(command, ["--version"], {
    env: environment,
  }).pipe(
    Effect.flatMap((spawnCommand) =>
      spawnAndCollect(
        command,
        ChildProcess.make(spawnCommand.command, spawnCommand.args, {
          env: { ...environment, MUSE_NO_AUTO_UPDATE: "1" },
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
  return buildServerProvider({
    presentation: MUSE_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    probe: {
      installed: true,
      version,
      status: "ready",
      // Whether the local `muse` install is signed in is only observable by
      // opening a real MSP session (`session/start` + a turn) — deferred to
      // avoid making a background health check start a session, per
      // docs/internals/providers.md.
      auth: { status: "unknown" },
      message: "Muse Code CLI is installed. Sign-in status is checked when a session starts.",
    },
  });
});
