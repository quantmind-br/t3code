// @effect-diagnostics nodeBuiltinImport:off preferSchemaOverJson:off - test fixture, plain Node.
/**
 * msp-mock-host — a scriptable MSP host for `MuseAdapter` tests.
 *
 * Speaks enough of the Muse Session Protocol to exercise the adapter end to
 * end: newline-delimited JSON-RPC on stdio, the `initialize` -> `initialized`
 * handshake the real host gates every `session/*` behind, and a per-test
 * script of notifications.
 *
 * Configuration rides the environment so the fixture stays a plain executable:
 *   - `T3_MSP_REQUEST_LOG_PATH` — every received request appended as NDJSON.
 *   - `T3_MSP_ENV_LOG_PATH`     — selected env vars written once at startup.
 *   - `T3_MSP_SCRIPT_PATH`      — JSON file: `{ afterTurnStart: [...notifications],
 *                                 viewPages: { "<cursor>": { events, nextCursor } } }`.
 */
import * as NodeFS from "node:fs";

interface JsonRpcRequest {
  readonly id?: number | string;
  readonly method: string;
  readonly params?: Record<string, unknown>;
}

const requestLogPath = process.env.T3_MSP_REQUEST_LOG_PATH;
const envLogPath = process.env.T3_MSP_ENV_LOG_PATH;
const scriptPath = process.env.T3_MSP_SCRIPT_PATH;

interface MockScript {
  readonly afterTurnStart?: ReadonlyArray<{ method: string; params: Record<string, unknown> }>;
  readonly viewPages?: Record<
    string,
    {
      events: ReadonlyArray<{ method: string; params: Record<string, unknown> }>;
      nextCursor: string | null;
    }
  >;
}

const script: MockScript = scriptPath
  ? (JSON.parse(NodeFS.readFileSync(scriptPath, "utf8")) as MockScript)
  : {};

if (envLogPath) {
  NodeFS.writeFileSync(
    envLogPath,
    JSON.stringify({
      XDG_CONFIG_HOME: process.env.XDG_CONFIG_HOME ?? null,
      XDG_DATA_HOME: process.env.XDG_DATA_HOME ?? null,
      MUSE_AUTH_PATH: process.env.MUSE_AUTH_PATH ?? null,
    }),
    "utf8",
  );
}

const write = (payload: unknown) => {
  process.stdout.write(`${JSON.stringify(payload)}\n`);
};

const notify = (method: string, params: Record<string, unknown>) => {
  write({ jsonrpc: "2.0", method, params });
};

const respond = (id: number | string, result: unknown) => {
  write({ jsonrpc: "2.0", id, result });
};

const fail = (id: number | string, code: number, message: string, kind: string) => {
  write({ jsonrpc: "2.0", id, error: { code, message, data: { kind } } });
};

/** Frozen so the fixture never depends on wall-clock time. */
const FIXED_TIMESTAMP = "1970-01-01T00:00:00.000Z";
const SESSION_ID = "11111111-1111-7111-8111-111111111111";
const TURN_ID = "22222222-2222-7222-8222-222222222222";

let initialized = false;

const handle = (request: JsonRpcRequest) => {
  if (requestLogPath) {
    NodeFS.appendFileSync(requestLogPath, `${JSON.stringify(request)}\n`, "utf8");
  }
  const { id, method, params = {} } = request;
  if (id === undefined) return; // notification from the client

  if (method !== "initialize" && !initialized) {
    fail(id, -32002, "not initialized", "notInitialized");
    return;
  }

  switch (method) {
    case "initialize":
      respond(id, {
        experimentalApi: false,
        grantedCapabilities: [],
        museHome: process.env.XDG_DATA_HOME
          ? `${process.env.XDG_DATA_HOME}/muse`
          : "/tmp/mock-muse-home",
        platformFamily: "unix",
        platformOs: "linux",
        schema: { fingerprint: "mock", version: 1 },
        serverInfo: { name: "msp-mock-host", version: "0.0.0" },
        userAgent: "msp-mock-host",
      });
      initialized = true;
      return;

    case "session/start":
      respond(id, {
        session: {
          activeTurnId: null,
          approvalMode: params.approvalMode ?? "denyUnmatched",
          createdAt: FIXED_TIMESTAMP,
          forkedFrom: null,
          modelId: "mock-model",
          path: "/tmp/mock-session.jsonl",
          providerId: "mock",
          sessionId: SESSION_ID,
          status: "idle",
          turnCount: 0,
          updatedAt: FIXED_TIMESTAMP,
          workspaceRoot: (params.workspaceRoot as string | undefined) ?? null,
        },
        viewCursor: "cursor-0",
      });
      return;

    case "turn/start": {
      respond(id, {
        commandId: String(params.commandId ?? ""),
        disposition: "started",
        startedNewTurn: true,
        status: "accepted",
        turnId: TURN_ID,
      });
      notify("turn/started", {
        sessionId: SESSION_ID,
        turnId: TURN_ID,
        viewCursor: "cursor-1",
        sourceRange: { from: 1, to: 1 },
      });
      for (const event of script.afterTurnStart ?? []) {
        notify(event.method, { sessionId: SESSION_ID, ...event.params });
      }
      // Always terminate the turn last, so a test has one deterministic
      // settle point after every scripted notification (including a
      // `view/gap` whose recovery runs inline on the consumer).
      notify("turn/completed", {
        sessionId: SESSION_ID,
        turnId: TURN_ID,
        terminal: "completed",
        viewCursor: "cursor-last",
        sourceRange: { from: 99, to: 99 },
      });
      return;
    }

    case "view/page": {
      const cursor = typeof params.cursor === "string" ? params.cursor : "";
      const page = script.viewPages?.[cursor];
      respond(id, page ? page : { events: [], nextCursor: null });
      return;
    }

    case "turn/interrupt":
      respond(id, { status: "accepted" });
      return;

    default:
      respond(id, {});
      return;
  }
};

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string) => {
  buffer += chunk;
  let index: number;
  while ((index = buffer.indexOf("\n")) >= 0) {
    const line = buffer.slice(0, index);
    buffer = buffer.slice(index + 1);
    if (line.trim().length === 0) continue;
    handle(JSON.parse(line) as JsonRpcRequest);
  }
});
process.stdin.on("end", () => process.exit(0));
