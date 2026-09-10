/**
 * MSP idempotency handles (`commandId`, `sessionId` when the client mints
 * one) must be UUIDv7 (48-bit ms timestamp, version nibble `7`, variant
 * nibble `8-b`, 62 bits of randomness) — `session/start` rejects anything
 * else with `invalidParams`. `effect/Crypto` only exposes v4 generation, so
 * this is a small dependency-free v7 generator over the Effect `Clock`/
 * `Random` services (never global `Date`/`Math.random`), validated against
 * a live `muse serve` host during integration testing.
 */
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";

const hex16 = (value: number, bits: number): string =>
  (value & (2 ** bits - 1)).toString(16).padStart(Math.ceil(bits / 4), "0");

export const randomUuidV7: Effect.Effect<string> = Effect.gen(function* () {
  const now = yield* Clock.currentTimeMillis;
  const ts = Math.trunc(now).toString(16).padStart(12, "0").slice(-12);
  const randA = hex16(yield* Random.nextIntBetween(0, 0x1000), 12); // 3 hex digits
  // 62 bits of rand_b: the variant nibble ("8") supplies 2 fixed high bits,
  // the remaining 60 bits come from 4 random 16-bit words.
  const words = yield* Effect.forEach([0, 1, 2, 3], () => Random.nextIntBetween(0, 0x10000));
  const randB = words
    .map((word) => hex16(word, 16))
    .join("")
    .slice(0, 15);
  const h = `${ts}7${randA}8${randB}`;
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
});
