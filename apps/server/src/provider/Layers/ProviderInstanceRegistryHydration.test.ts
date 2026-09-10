/**
 * The settings UI and the provider registry both hang off one derivation:
 * `deriveProviderInstanceConfigMap` synthesizes a driver's default instance
 * from the legacy `settings.providers.<kind>` blob when no explicit
 * `providerInstances` entry exists.
 *
 * A built-in driver with no legacy blob is therefore dropped silently — no
 * registry instance, no runtime, and no configurable row in Settings →
 * Providers. Nothing else fails loudly, so the two shipped lists are asserted
 * to agree here rather than trusted to.
 */
import { describe, expect, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, defaultInstanceIdForDriver } from "@t3tools/contracts";

import { BUILT_IN_DRIVERS } from "../builtInDrivers.ts";
import { deriveProviderInstanceConfigMap } from "./ProviderInstanceRegistryHydration.ts";

describe("deriveProviderInstanceConfigMap", () => {
  it("derives an instance for every built-in driver from the shipped defaults", () => {
    const derived = deriveProviderInstanceConfigMap(DEFAULT_SERVER_SETTINGS);

    for (const driver of BUILT_IN_DRIVERS) {
      const instanceId = defaultInstanceIdForDriver(driver.driverKind);
      expect(derived[instanceId], `no default instance for ${driver.driverKind}`).toBeDefined();
      expect(derived[instanceId]?.driver).toBe(driver.driverKind);
    }
  });
});
