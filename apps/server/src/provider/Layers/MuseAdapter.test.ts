// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - fixture wiring in a test.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import {
  ProviderInstanceId,
  ThreadId,
  type ChatAttachment,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import { execScriptSource, writeFakeCli } from "../../testUtils/fakeCli.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { makeMuseAdapter } from "./MuseAdapter.ts";
import { makeMuseEnvironment, resolveMuseProfilePaths } from "./MuseEnvironment.ts";

const __dirname = NodePath.dirname(NodeURL.fileURLToPath(import.meta.url));
const mockHostPath = NodePath.join(__dirname, "../../../scripts/msp-mock-host.ts");

const SESSION_ID = "11111111-1111-7111-8111-111111111111";

interface MockScript {
  readonly afterTurnStart?: ReadonlyArray<{
    readonly method: string;
    readonly params: Record<string, unknown>;
  }>;
  readonly viewPages?: Record<
    string,
    {
      readonly events: ReadonlyArray<{
        readonly method: string;
        readonly params: Record<string, unknown>;
      }>;
      readonly nextCursor: string | null;
    }
  >;
}

interface MockHost {
  readonly binaryPath: string;
  readonly requestLogPath: string;
  readonly envLogPath: string;
  readonly attachmentsDir: string;
}

const makeMockHost = (script: MockScript): Effect.Effect<MockHost> =>
  Effect.promise(async () => {
    const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "muse-adapter-test-"));
    const scriptPath = NodePath.join(dir, "script.json");
    const requestLogPath = NodePath.join(dir, "requests.ndjson");
    const envLogPath = NodePath.join(dir, "env.json");
    const attachmentsDir = NodePath.join(dir, "attachments");
    await NodeFSP.mkdir(attachmentsDir, { recursive: true });
    await NodeFSP.writeFile(scriptPath, JSON.stringify(script), "utf8");
    const binaryPath = writeFakeCli({
      directory: dir,
      name: "fake-muse",
      env: {
        T3_MSP_SCRIPT_PATH: scriptPath,
        T3_MSP_REQUEST_LOG_PATH: requestLogPath,
        T3_MSP_ENV_LOG_PATH: envLogPath,
      },
      source: execScriptSource({ scriptPath: mockHostPath }),
    });
    return { binaryPath, requestLogPath, envLogPath, attachmentsDir };
  });

const readRequests = (path: string): Effect.Effect<Array<Record<string, unknown>>> =>
  Effect.promise(async () => {
    const raw = await NodeFSP.readFile(path, "utf8").catch(() => "");
    return raw
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .map((line) => JSON.parse(line) as Record<string, unknown>);
  });

const museAdapterTestLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-muse-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

/**
 * Starts the adapter against a mock host, collects every runtime event it
 * emits, runs `body`, and settles on the turn's own completion.
 *
 * The drain is forked with `startImmediately` so its PubSub subscription is
 * live before the session starts, and the wait is a `Deferred` the drain
 * itself completes — `it.effect` runs on a virtual `TestClock`, so a sleep
 * here would never elapse. The mock host always emits `turn/completed` last,
 * and the adapter's notification consumer is sequential, so observing
 * `turn.completed` proves every scripted notification (and any `view/gap`
 * recovery they triggered) has already been applied.
 */
const withAdapter = <A>(
  host: MockHost,
  threadId: ThreadId,
  body: (adapter: ProviderAdapterShape<never>) => Effect.Effect<A>,
  options?: { readonly homePath?: string },
) =>
  Effect.gen(function* () {
    const adapter = yield* makeMuseAdapter({
      binaryPath: host.binaryPath,
      customModels: [],
      environment: process.env,
      instanceId: ProviderInstanceId.make("muse-test"),
      homePath: options?.homePath,
      attachmentsDir: host.attachmentsDir,
    }).pipe(Effect.orDie);

    const events = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
    const turnSettled = yield* Deferred.make<void>();
    yield* adapter.streamEvents.pipe(
      Stream.runForEach((event) =>
        Effect.gen(function* () {
          yield* Ref.update(events, (current) => [...current, event]);
          if (event.type === "turn.completed") yield* Deferred.succeed(turnSettled, undefined);
        }),
      ),
      Effect.forkScoped({ startImmediately: true }),
    );

    yield* adapter
      .startSession({ threadId, cwd: process.cwd(), runtimeMode: "approval-required" })
      .pipe(Effect.orDie);
    const value = yield* body(adapter as never);
    yield* Deferred.await(turnSettled);
    yield* adapter.stopSession(threadId).pipe(Effect.orDie);
    return { value, events: yield* Ref.get(events) };
  }).pipe(Effect.scoped);

it("derives isolated XDG/auth paths from a Muse profile root", () => {
  const profile = resolveMuseProfilePaths("/tmp/work-account");
  assert.deepEqual(profile, {
    configHome: NodePath.join("/tmp/work-account", "config"),
    dataHome: NodePath.join("/tmp/work-account", "data"),
    authPath: NodePath.join("/tmp/work-account", "config", "muse", "auth.json"),
  });
  assert.isUndefined(resolveMuseProfilePaths(""));
  assert.isUndefined(resolveMuseProfilePaths("   "));
  assert.isUndefined(resolveMuseProfilePaths(undefined));
});

it("leaves the environment untouched when no Muse profile is configured", () => {
  const base = { PATH: "/usr/bin", XDG_CONFIG_HOME: "/home/u/.config" };
  assert.strictEqual(makeMuseEnvironment(base, undefined), base);
  assert.strictEqual(makeMuseEnvironment(base, "  "), base);
});

it("two Muse profiles never share a credential or data directory", () => {
  const a = makeMuseEnvironment({}, "/tmp/acct-a");
  const b = makeMuseEnvironment({}, "/tmp/acct-b");
  assert.notStrictEqual(a.XDG_CONFIG_HOME, b.XDG_CONFIG_HOME);
  assert.notStrictEqual(a.XDG_DATA_HOME, b.XDG_DATA_HOME);
  assert.notStrictEqual(a.MUSE_AUTH_PATH, b.MUSE_AUTH_PATH);
});

it.layer(museAdapterTestLayer)("MuseAdapter", (it) => {
  it.effect("sends image attachments as native MSP image parts", () =>
    Effect.gen(function* () {
      const host = yield* makeMockHost({});
      const threadId = ThreadId.make("muse-attachments");
      const pngBytes = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x01, 0x02]);
      const attachmentId = "att_image_one";
      yield* Effect.promise(() =>
        NodeFSP.writeFile(NodePath.join(host.attachmentsDir, `${attachmentId}.png`), pngBytes),
      );
      const attachment = {
        type: "image",
        id: attachmentId,
        name: "shot.png",
        mimeType: "image/png",
        sizeBytes: pngBytes.byteLength,
      } as unknown as ChatAttachment;

      const { events } = yield* withAdapter(host, threadId, (adapter) =>
        adapter
          .sendTurn({ threadId, input: "look at this", attachments: [attachment] })
          .pipe(Effect.orDie),
      );

      const requests = yield* readRequests(host.requestLogPath);
      const turnStart = requests.find((request) => request.method === "turn/start");
      assert.isDefined(turnStart, "turn/start was never sent");
      const input = (turnStart!.params as { input: Array<Record<string, unknown>> }).input;
      assert.deepEqual(input, [
        { type: "text", text: "look at this" },
        { type: "image", base64Data: pngBytes.toString("base64"), mediaType: "image/png" },
      ]);
      // The old text-only warning must be gone.
      assert.isUndefined(
        events.find(
          (event) =>
            event.type === "runtime.warning" &&
            String((event.payload as { message?: unknown }).message ?? "").includes("text-only"),
        ),
      );
    }),
  );

  it.effect("sends an image-only turn with no text part", () =>
    Effect.gen(function* () {
      const host = yield* makeMockHost({});
      const threadId = ThreadId.make("muse-image-only");
      const bytes = Buffer.from([1, 2, 3, 4]);
      yield* Effect.promise(() =>
        NodeFSP.writeFile(NodePath.join(host.attachmentsDir, "att_only.png"), bytes),
      );
      const attachment = {
        type: "image",
        id: "att_only",
        name: "only.png",
        mimeType: "image/png",
        sizeBytes: bytes.byteLength,
      } as unknown as ChatAttachment;

      yield* withAdapter(host, threadId, (adapter) =>
        adapter.sendTurn({ threadId, input: "", attachments: [attachment] }).pipe(Effect.orDie),
      );

      const requests = yield* readRequests(host.requestLogPath);
      const turnStart = requests.find((request) => request.method === "turn/start");
      const input = (turnStart!.params as { input: Array<Record<string, unknown>> }).input;
      assert.deepEqual(input, [
        { type: "image", base64Data: bytes.toString("base64"), mediaType: "image/png" },
      ]);
    }),
  );

  it.effect("projects subagent items onto agent task events", () =>
    Effect.gen(function* () {
      const host = yield* makeMockHost({
        afterTurnStart: [
          {
            method: "item/started",
            params: {
              viewCursor: "cursor-2",
              sourceRange: { from: 2, to: 2 },
              item: {
                itemId: "item-subagent-1",
                kind: "subagent",
                revision: 1,
                status: "inProgress",
                turnId: "22222222-2222-7222-8222-222222222222",
                objective: "Audit the auth layer",
                role: "reviewer",
                agentPath: "/root/reviewer",
                subagentId: "sub-1",
                childSessionId: "child-session-1",
                controlStatus: "running",
                depth: 1,
              },
            },
          },
          {
            method: "item/completed",
            params: {
              viewCursor: "cursor-3",
              sourceRange: { from: 3, to: 3 },
              item: {
                itemId: "item-subagent-1",
                kind: "subagent",
                revision: 2,
                status: "completed",
                turnId: "22222222-2222-7222-8222-222222222222",
                objective: "Audit the auth layer",
                role: "reviewer",
                agentPath: "/root/reviewer",
                subagentId: "sub-1",
                childSessionId: "child-session-1",
                controlStatus: "closed",
                depth: 1,
                durationMs: 4200,
                usage: {
                  cachedTokens: 10,
                  inputTokens: 100,
                  outputTokens: 40,
                  reasoningTokens: 7,
                },
              },
            },
          },
        ],
      });
      const threadId = ThreadId.make("muse-subagent");

      const { events } = yield* withAdapter(host, threadId, (adapter) =>
        adapter.sendTurn({ threadId, input: "delegate" }).pipe(Effect.orDie),
      );

      const itemStarted = events.find(
        (event) => event.type === "item.started" && String(event.itemId) === "item-subagent-1",
      );
      assert.isDefined(itemStarted, "item.started for the subagent was never emitted");
      assert.strictEqual(
        (itemStarted!.payload as { itemType: string }).itemType,
        "collab_agent_tool_call",
        "subagent items must not render as a generic tool call",
      );
      assert.strictEqual(
        (itemStarted!.payload as { title?: string }).title,
        "Audit the auth layer",
      );

      const started = events.filter((event) => event.type === "task.started");
      assert.lengthOf(started, 1, "the subagent must open exactly one task row");
      assert.deepInclude(started[0]!.payload as Record<string, unknown>, {
        taskId: "item-subagent-1",
        description: "Audit the auth layer",
        taskType: "subagent",
        agentKind: "agent",
        agentId: "sub-1",
        role: "reviewer",
        agentPath: "/root/reviewer",
      });

      const completed = events.filter((event) => event.type === "task.completed");
      assert.lengthOf(completed, 1);
      const completedPayload = completed[0]!.payload as Record<string, unknown>;
      assert.strictEqual(completedPayload.status, "completed");
      assert.deepEqual(completedPayload.typedUsage, {
        totalTokens: 140,
        inputTokens: 100,
        outputTokens: 40,
        cachedInputTokens: 10,
        reasoningOutputTokens: 7,
        durationMs: 4200,
      });
    }),
  );

  it.effect("projects workflow children onto their own parented task rows", () =>
    Effect.gen(function* () {
      const host = yield* makeMockHost({
        afterTurnStart: [
          {
            method: "item/completed",
            params: {
              viewCursor: "cursor-2",
              sourceRange: { from: 2, to: 2 },
              item: {
                itemId: "item-workflow-1",
                kind: "workflow",
                revision: 3,
                status: "completed",
                turnId: "22222222-2222-7222-8222-222222222222",
                entryId: "review-changes",
                scriptId: "review-changes.js",
                workflowRunId: "wf_run_1",
                message: "3 findings confirmed",
                children: [
                  {
                    attempt: 1,
                    childId: "review:bugs",
                    label: "review bugs",
                    phase: "Review",
                    status: "completed",
                    terminal: "completed",
                    durationMs: 1200,
                    usage: {
                      cachedTokens: 0,
                      inputTokens: 20,
                      outputTokens: 5,
                      reasoningTokens: 0,
                    },
                  },
                  {
                    attempt: 1,
                    childId: "verify:bugs",
                    label: "verify bugs",
                    phase: "Verify",
                    status: "running",
                  },
                ],
              },
            },
          },
        ],
      });
      const threadId = ThreadId.make("muse-workflow");

      const { events } = yield* withAdapter(host, threadId, (adapter) =>
        adapter.sendTurn({ threadId, input: "run the workflow" }).pipe(Effect.orDie),
      );

      const started = events.filter((event) => event.type === "task.started");
      assert.deepEqual(
        started.map((event) => (event.payload as { taskId: string }).taskId),
        ["item-workflow-1", "item-workflow-1:review:bugs:1", "item-workflow-1:verify:bugs:1"],
      );

      const workflowStart = started[0]!.payload as Record<string, unknown>;
      assert.strictEqual(workflowStart.taskType, "local_workflow");
      assert.strictEqual(workflowStart.workflowName, "review-changes");
      assert.deepEqual(workflowStart.phases, [
        { index: 0, title: "Review" },
        { index: 1, title: "Verify" },
      ]);
      assert.deepEqual(workflowStart.runHandles, {
        runId: "wf_run_1",
        scriptPath: "review-changes.js",
      });

      const childStart = started[1]!.payload as Record<string, unknown>;
      assert.strictEqual(childStart.parentAgentId, "item-workflow-1");
      assert.strictEqual(childStart.phaseTitle, "Review");
      assert.strictEqual(childStart.phaseIndex, 0);
      assert.strictEqual(childStart.attempt, 1);

      // The still-running child reports progress, never a completion.
      const childCompletions = events
        .filter((event) => event.type === "task.completed")
        .map((event) => (event.payload as { taskId: string }).taskId);
      assert.include(childCompletions, "item-workflow-1:review:bugs:1");
      assert.notInclude(childCompletions, "item-workflow-1:verify:bugs:1");
      const runningProgress = events.find(
        (event) =>
          event.type === "task.progress" &&
          (event.payload as { taskId: string }).taskId === "item-workflow-1:verify:bugs:1",
      );
      assert.isDefined(runningProgress);
      assert.strictEqual((runningProgress!.payload as { status: string }).status, "running");
    }),
  );

  it.effect("splices a view/gap hole back in through view/page", () =>
    Effect.gen(function* () {
      const missedItem = (itemId: string, viewCursor: string) => ({
        method: "item/completed",
        params: {
          sessionId: SESSION_ID,
          viewCursor,
          sourceRange: { from: 1, to: 1 },
          item: {
            itemId,
            kind: "agentMessage",
            revision: 1,
            status: "completed",
            turnId: "22222222-2222-7222-8222-222222222222",
            text: `recovered ${itemId}`,
          },
        },
      });

      const host = yield* makeMockHost({
        afterTurnStart: [
          {
            method: "view/gap",
            params: { after: "cursor-1", next: "cursor-4" },
          },
          {
            method: "item/completed",
            params: {
              viewCursor: "cursor-4",
              sourceRange: { from: 4, to: 4 },
              item: {
                itemId: "item-live",
                kind: "agentMessage",
                revision: 1,
                status: "completed",
                turnId: "22222222-2222-7222-8222-222222222222",
                text: "live",
              },
            },
          },
        ],
        viewPages: {
          "cursor-1": {
            events: [missedItem("item-missed-a", "cursor-2")],
            nextCursor: "cursor-2",
          },
          "cursor-2": {
            events: [
              missedItem("item-missed-b", "cursor-3"),
              // `cursor-4` is the exclusive upper bound: already delivered
              // live, so it must be discarded rather than replayed.
              missedItem("item-live", "cursor-4"),
            ],
            nextCursor: "cursor-4",
          },
        },
      });
      const threadId = ThreadId.make("muse-view-gap");

      const { events } = yield* withAdapter(host, threadId, (adapter) =>
        adapter.sendTurn({ threadId, input: "go" }).pipe(Effect.orDie),
      );

      const requests = yield* readRequests(host.requestLogPath);
      const pageCursors = requests
        .filter((request) => request.method === "view/page")
        .map((request) => (request.params as { cursor?: string }).cursor);
      assert.deepEqual(pageCursors, ["cursor-1", "cursor-2"], "the gap must be paged forward");

      const recoveredIds = events
        .filter((event) => event.type === "item.completed")
        .map((event) => String(event.itemId));
      assert.deepEqual(
        recoveredIds,
        ["item-missed-a", "item-missed-b", "item-live"],
        "both dropped items are spliced in, and the live one is not duplicated",
      );

      // Real recovery replaces the old "some activity may be missing" warning.
      assert.isUndefined(
        events.find(
          (event) =>
            event.type === "runtime.warning" &&
            String((event.payload as { message?: unknown }).message ?? "").includes("view/gap"),
        ),
      );
    }),
  );

  it.effect("warns, with a count, when a view/gap cannot be fully recovered", () =>
    Effect.gen(function* () {
      const host = yield* makeMockHost({
        afterTurnStart: [
          { method: "view/gap", params: { after: "cursor-unservable", next: "cursor-9" } },
        ],
        // No page for "cursor-unservable": the host serves an empty terminal
        // page, so the walk ends without ever reaching `next`.
        viewPages: {},
      });
      const threadId = ThreadId.make("muse-view-gap-partial");

      const { events } = yield* withAdapter(host, threadId, (adapter) =>
        adapter.sendTurn({ threadId, input: "go" }).pipe(Effect.orDie),
      );

      const requests = yield* readRequests(host.requestLogPath);
      assert.isTrue(
        requests.some((request) => request.method === "view/page"),
        "recovery must still be attempted",
      );
      // An exhausted-but-clean walk is not an error; nothing more was servable.
      assert.isUndefined(events.find((event) => event.type === "runtime.warning"));
    }),
  );

  it.effect("runs the host under the configured account's XDG directories", () =>
    Effect.gen(function* () {
      const host = yield* makeMockHost({});
      const profileRoot = NodePath.join(NodeOS.tmpdir(), "muse-profile-account-a");
      const threadId = ThreadId.make("muse-multi-account");

      yield* withAdapter(
        host,
        threadId,
        (adapter) => adapter.sendTurn({ threadId, input: "hi" }).pipe(Effect.orDie),
        { homePath: profileRoot },
      );

      const observed = yield* Effect.promise(async () =>
        JSON.parse(await NodeFSP.readFile(host.envLogPath, "utf8")),
      );
      assert.deepEqual(observed, {
        XDG_CONFIG_HOME: NodePath.join(profileRoot, "config"),
        XDG_DATA_HOME: NodePath.join(profileRoot, "data"),
        MUSE_AUTH_PATH: NodePath.join(profileRoot, "config", "muse", "auth.json"),
      });
    }),
  );
});
