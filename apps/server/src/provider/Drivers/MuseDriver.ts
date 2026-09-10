/**
 * MuseDriver — `ProviderDriver` for Muse Code, over the Muse Session
 * Protocol (`packages/effect-msp`). Mirrors the Grok/Cursor driver shape:
 * a plain value whose `create()` bundles `snapshot`/`adapter`/
 * `textGeneration` closures over the per-instance `MuseSettings`.
 *
 * Off by default (`enabled: false`), same as Cursor/Grok/OpenCode — a new
 * driver earns its way onto every install rather than probing on first
 * run. Authentication is whatever the configured `binaryPath` install is
 * already signed into (`muse login`); this driver does not manage sign-in.
 *
 * @module provider/Drivers/MuseDriver
 */
import { MuseSettings, ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";

import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeMuseTextGeneration } from "../../textGeneration/MuseTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeMuseAdapter } from "../Layers/MuseAdapter.ts";
import {
  buildInitialMuseProviderSnapshot,
  checkMuseProviderStatus,
} from "../Layers/MuseProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";

const decodeMuseSettings = Schema.decodeSync(MuseSettings);
const DRIVER_KIND = ProviderDriverKind.make("muse");
const MAINTENANCE_CAPABILITIES = makeManualOnlyProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
});

export type MuseDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | ServerConfig
  | ServerSettingsService;

export const MuseDriver: ProviderDriver<MuseSettings, MuseDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Muse Code",
    supportsMultipleInstances: true,
  },
  configSchema: MuseSettings,
  defaultConfig: (): MuseSettings => decodeMuseSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fileSystem = yield* FileSystem.FileSystem;
      const serverSettings = yield* ServerSettingsService;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies MuseSettings;

      const adapter = yield* makeMuseAdapter({
        binaryPath: effectiveConfig.binaryPath,
        customModels: effectiveConfig.customModels,
        environment: processEnv,
        instanceId,
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

      const textGeneration = yield* makeMuseTextGeneration(effectiveConfig, processEnv).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(FileSystem.FileSystem, fileSystem),
      );

      const checkProvider = checkMuseProviderStatus(effectiveConfig, processEnv).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<MuseSettings>>({
        resolveMaintenance: () => Effect.succeed(MAINTENANCE_CAPABILITIES),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialMuseProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Muse snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
