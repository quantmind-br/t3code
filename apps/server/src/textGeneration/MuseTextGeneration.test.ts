import { describe, expect, it } from "vite-plus/test";

import { isRelativeExecutablePath } from "./MuseTextGeneration.ts";

// Metadata generation spawns `muse serve` in an isolated temp dir, so a
// relative `binaryPath` (meaningful against the repository the interactive
// adapter runs in) must be anchored to the repo cwd first — or it silently
// resolves to nothing inside the empty temp dir. This is the classifier
// that decides which configured values need that anchoring.
describe("isRelativeExecutablePath", () => {
  it("treats bare command names as PATH lookups, not relative paths", () => {
    expect(isRelativeExecutablePath("muse")).toBe(false);
    expect(isRelativeExecutablePath("muse.exe")).toBe(false);
    expect(isRelativeExecutablePath("")).toBe(false);
  });

  it("treats absolute paths on every platform as already anchored", () => {
    expect(isRelativeExecutablePath("/opt/muse/bin/muse")).toBe(false);
    expect(isRelativeExecutablePath("C:\\Program Files\\muse\\muse.exe")).toBe(false);
    expect(isRelativeExecutablePath("D:/tools/muse.exe")).toBe(false);
    expect(isRelativeExecutablePath("\\\\server\\share\\muse.exe")).toBe(false);
  });

  it("flags cwd-relative paths that only resolve against the repository", () => {
    expect(isRelativeExecutablePath("./bin/muse")).toBe(true);
    expect(isRelativeExecutablePath("../tools/muse")).toBe(true);
    expect(isRelativeExecutablePath("bin/muse")).toBe(true);
    expect(isRelativeExecutablePath(".\\bin\\muse.exe")).toBe(true);
  });
});
