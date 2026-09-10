/**
 * Minimal MSP host over real stdio, for exercising the client against an
 * actual child process (pipe lifecycle, stdin end-on-done, multi-frame
 * writes) — the in-memory Stdio harness cannot reproduce those.
 *
 * Mirrors the real host's `guard_initialized` gate: every `session/*`
 * method fails with `notInitialized` until the client has sent the
 * `initialized` notification after `initialize`.
 */
import * as readline from "node:readline";

let initialized = false;
let sessionCounter = 0;

const write = (message: unknown) => {
  process.stdout.write(`${JSON.stringify(message)}\n`);
};

const session = (sessionId: string) => ({
  activeTurnId: null,
  createdAt: "2026-01-01T00:00:00Z",
  forkedFrom: null,
  modelId: null,
  path: `/tmp/${sessionId}.jsonl`,
  providerId: null,
  sessionId,
  status: "idle",
  turnCount: 0,
  updatedAt: "2026-01-01T00:00:00Z",
  workspaceRoot: "/tmp/work",
});

const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
rl.on("line", (line) => {
  if (line.trim().length === 0) return;
  const message = JSON.parse(line) as {
    id?: number | string;
    method: string;
    params?: Record<string, unknown>;
  };
  switch (message.method) {
    case "initialize":
      write({
        id: message.id,
        result: {
          experimentalApi: false,
          grantedCapabilities: [],
          museHome: "/tmp/muse-home",
          platformFamily: "unix",
          platformOs: "linux",
          schema: { fingerprint: "sha256:mock", version: 1 },
          serverInfo: { name: "muse-mock", version: "0.0.0" },
          userAgent: "muse-mock/0.0.0",
        },
      });
      return;
    case "initialized":
      initialized = true;
      return;
    case "session/start": {
      if (!initialized) {
        write({
          id: message.id,
          error: { code: -32600, message: "Not initialized", data: { kind: "notInitialized" } },
        });
        return;
      }
      sessionCounter += 1;
      const sessionId = `session-${sessionCounter}`;
      write({ id: message.id, result: { session: session(sessionId), viewCursor: "v:0:0" } });
      return;
    }
    default:
      write({
        id: message.id,
        error: { code: -32601, message: "Method not found", data: { kind: "methodNotFound" } },
      });
  }
});
rl.on("close", () => process.exit(0));
