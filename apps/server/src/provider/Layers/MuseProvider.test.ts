// @effect-diagnostics nodeBuiltinImport:off - resolves the mock MSP host script path relative to this test file.
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { MuseSettings } from "@t3tools/contracts";

import { checkMuseProviderStatus, museModelsFromCatalog } from "./MuseProvider.ts";
import { execScriptSource, writeFakeCli } from "../../testUtils/fakeCli.ts";

const decodeMuseSettings = Schema.decodeSync(MuseSettings);
const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockHostPath = NodePath.resolve(__dirname, "../../../scripts/msp-mock-host.ts");

const catalogEntry = (modelId: string, isDefault = false) => ({
  contextLimit: 1007997,
  description: null,
  displayLabel: modelId,
  isActive: false,
  isDefault,
  modelId,
  outputLimit: 128000,
  profileId: "tbh",
  providerId: "meta",
  releaseDate: "2026-09-02",
});

describe("museModelsFromCatalog", () => {
  it("projects catalog rows in order, keeping the catalog default", () => {
    const models = museModelsFromCatalog([
      catalogEntry("muse-spark-1.3"),
      catalogEntry("muse-spark-1.3-contributor", true),
    ]);
    expect(models.map((model) => model.slug)).toEqual([
      "muse-spark-1.3",
      "muse-spark-1.3-contributor",
    ]);
    expect(models.map((model) => model.isDefault ?? false)).toEqual([false, true]);
    expect(models.every((model) => !model.isCustom)).toBe(true);
  });

  it("drops blank and duplicate ids", () => {
    const models = museModelsFromCatalog([
      catalogEntry("muse-spark-1.3"),
      catalogEntry(" "),
      catalogEntry("muse-spark-1.3"),
    ]);
    expect(models.map((model) => model.slug)).toEqual(["muse-spark-1.3"]);
  });
});

it.layer(NodeServices.layer)("checkMuseProviderStatus", (it) => {
  // `--version` answers the health check; `serve` execs the mock MSP host so
  // `model/list` returns a catalog without any session being started.
  const writeFakeMuseCli = (input: { readonly serve: boolean }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3code-muse-probe-" });
      const requestLogPath = NodePath.join(dir, "requests.ndjson");
      const binaryPath = writeFakeCli({
        directory: dir,
        name: "muse",
        env: { T3_MSP_REQUEST_LOG_PATH: requestLogPath },
        source: [
          'if (process.argv[2] === "--version") {',
          '  process.stdout.write("Muse Code 1.1.1 (1.1.1-R2514.1)\\n");',
          "  process.exit(0);",
          "}",
          'if (process.argv[2] !== "serve") process.exit(1);',
          ...(input.serve
            ? [execScriptSource({ scriptPath: mockHostPath })]
            : ["process.exit(3);"]),
          "",
        ].join("\n"),
      });
      return { binaryPath, requestLogPath };
    });

  it.effect("reports the binary as missing when the binary path does not resolve", () =>
    Effect.gen(function* () {
      const snapshot = yield* checkMuseProviderStatus(
        decodeMuseSettings({ enabled: true, binaryPath: "/definitely/not/installed/muse" }),
      );
      expect(snapshot.installed).toBe(false);
      expect(snapshot.status).toBe("error");
      expect(snapshot.models).toEqual([]);
    }),
  );

  it.effect("reports ready with the model/list catalog, without starting a session", () =>
    Effect.gen(function* () {
      const { snapshot, requests } = yield* Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const { binaryPath, requestLogPath } = yield* writeFakeMuseCli({ serve: true });
          const snapshot = yield* checkMuseProviderStatus(
            decodeMuseSettings({
              enabled: true,
              binaryPath,
              customModels: [{ slug: "my-custom" }],
            }),
          );
          const log = yield* fs.readFileString(requestLogPath);
          const requests = log
            .split("\n")
            .filter((line) => line.length > 0)
            .map(
              (line) =>
                Schema.decodeUnknownSync(Schema.Struct({ method: Schema.String }))(JSON.parse(line))
                  .method,
            );
          return { snapshot, requests };
        }),
      );

      expect(snapshot.status).toBe("ready");
      expect(snapshot.version).toBe("1.1.1");
      expect(snapshot.models.map((model) => model.slug)).toEqual([
        "muse-spark-1.3",
        "muse-spark-1.3-contributor",
        "my-custom",
      ]);
      expect(snapshot.models.find((model) => model.isDefault)?.slug).toBe(
        "muse-spark-1.3-contributor",
      );
      expect(snapshot.models.find((model) => model.slug === "my-custom")?.isCustom).toBe(true);
      // The probe is a query: handshake + catalog, no `session/*`.
      expect(requests).toEqual(["initialize", "initialized", "model/list"]);
    }),
  );

  it.effect("degrades to custom models with a warning when the host cannot serve", () =>
    Effect.gen(function* () {
      const snapshot = yield* Effect.scoped(
        Effect.gen(function* () {
          const { binaryPath } = yield* writeFakeMuseCli({ serve: false });
          return yield* checkMuseProviderStatus(
            decodeMuseSettings({
              enabled: true,
              binaryPath,
              customModels: [{ slug: "my-custom" }],
            }),
          );
        }),
      );

      expect(snapshot.installed).toBe(true);
      expect(snapshot.status).toBe("warning");
      expect(snapshot.message).toMatch(/model catalog could not be read/);
      expect(snapshot.models.map((model) => model.slug)).toEqual(["my-custom"]);
    }),
  );
});
