import { describe, expect, it } from "vitest";
import { shouldGuardRedispatch } from "../services/dispatch-reconciler.js";

function terminalRun(
  id: string,
  createdAt: string,
  finishedAt: string | null = createdAt,
) {
  return {
    id,
    status: "timed_out",
    createdAt: new Date(createdAt),
    finishedAt: finishedAt ? new Date(finishedAt) : null,
  };
}

describe("shouldGuardRedispatch", () => {
  const now = new Date("2026-05-14T18:00:00.000Z");

  it("allows first dispatch when no prior terminal run exists", () => {
    expect(shouldGuardRedispatch([], now)).toEqual({
      guarded: false,
      reason: null,
    });
  });

  it("suppresses immediate redispatch after a terminal run", () => {
    expect(
      shouldGuardRedispatch([
        terminalRun("run-1", "2026-05-14T17:45:00.000Z"),
      ], now),
    ).toEqual({
      guarded: true,
      reason: "cooldown",
    });
  });

  it("allows retry after the cooldown expires", () => {
    expect(
      shouldGuardRedispatch([
        terminalRun("run-1", "2026-05-14T17:00:00.000Z"),
      ], now),
    ).toEqual({
      guarded: false,
      reason: null,
    });
  });

  it("caps repeated redispatches inside the guard window", () => {
    expect(
      shouldGuardRedispatch([
        terminalRun("run-3", "2026-05-14T17:00:00.000Z"),
        terminalRun("run-2", "2026-05-14T12:00:00.000Z"),
        terminalRun("run-1", "2026-05-14T06:30:00.000Z"),
      ], now),
    ).toEqual({
      guarded: true,
      reason: "run_limit",
    });
  });

  it("does not count old terminal runs outside the guard window", () => {
    expect(
      shouldGuardRedispatch([
        terminalRun("run-3", "2026-05-14T17:00:00.000Z"),
        terminalRun("run-2", "2026-05-14T05:30:00.000Z"),
        terminalRun("run-1", "2026-05-14T05:00:00.000Z"),
      ], now),
    ).toEqual({
      guarded: false,
      reason: null,
    });
  });
});
