// @effect-diagnostics nodeBuiltinImport:off
/**
 * MuseEnvironment — per-instance account/state isolation for spawned Muse
 * processes.
 *
 * Muse has no single `MUSE_HOME`. The launcher resolves credentials from
 * `MUSE_AUTH_PATH`, else `$XDG_CONFIG_HOME/muse/auth.json`, else
 * `$HOME/.config/muse/auth.json`; the binary itself reads `XDG_CONFIG_HOME`
 * and `XDG_DATA_HOME`, keeping its session index and model catalog under
 * `$XDG_DATA_HOME/muse` (verified against Muse Code 1.1.1: `initialize`
 * reports `museHome` = `$XDG_DATA_HOME/muse`).
 *
 * A `homePath` therefore has to drive all three, and both config and data must
 * be isolated together: sharing the session index across accounts would let
 * one instance resume another account's sessions.
 *
 * @module provider/Layers/MuseEnvironment
 */
import * as NodePath from "node:path";

import { expandHomePath } from "../../pathExpansion.ts";

/** Subdirectories of a `homePath` profile root. */
export const MUSE_PROFILE_CONFIG_DIRNAME = "config";
export const MUSE_PROFILE_DATA_DIRNAME = "data";

export interface MuseProfilePaths {
  /** `XDG_CONFIG_HOME` for the child: credentials live in `<configHome>/muse`. */
  readonly configHome: string;
  /** `XDG_DATA_HOME` for the child: session index and catalog live in `<dataHome>/muse`. */
  readonly dataHome: string;
  /** `MUSE_AUTH_PATH`, so the launcher agrees with the binary about the account. */
  readonly authPath: string;
}

/**
 * Resolves a configured `homePath` into the concrete directories the child
 * process needs. Returns `undefined` when no profile is configured, which
 * means "share the machine's default Muse account" — the behaviour every
 * existing instance already has.
 */
export const resolveMuseProfilePaths = (
  homePath: string | undefined,
): MuseProfilePaths | undefined => {
  const trimmed = homePath?.trim();
  if (!trimmed) return undefined;
  // Absolute, always. The processes that consume these variables do not share
  // a working directory — the adapter host runs in the project, text
  // generation runs in an isolated temp dir, and the health probe runs in the
  // server's cwd — so a relative `homePath` would silently give each of them a
  // different account. XDG also specifies that relative XDG_* paths are
  // invalid and must be ignored, which would fall back to the default account.
  const root = NodePath.resolve(expandHomePath(trimmed));
  const configHome = NodePath.join(root, MUSE_PROFILE_CONFIG_DIRNAME);
  return {
    configHome,
    dataHome: NodePath.join(root, MUSE_PROFILE_DATA_DIRNAME),
    authPath: NodePath.join(configHome, "muse", "auth.json"),
  };
};

/**
 * Builds the environment for a spawned Muse process. Without a configured
 * profile the caller's environment passes through untouched, so the default
 * account keeps working exactly as before.
 */
export const makeMuseEnvironment = (
  environment: NodeJS.ProcessEnv,
  homePath: string | undefined,
): NodeJS.ProcessEnv => {
  const profile = resolveMuseProfilePaths(homePath);
  if (!profile) return environment;
  return {
    ...environment,
    XDG_CONFIG_HOME: profile.configHome,
    XDG_DATA_HOME: profile.dataHome,
    MUSE_AUTH_PATH: profile.authPath,
  };
};
