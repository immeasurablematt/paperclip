import { and, desc, eq, inArray, isNotNull, isNull, lt, or, sql } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import { agents, heartbeatRuns, issues } from "@paperclipai/db";
import { logger } from "../middleware/logger.js";

// Re-dispatches todo issues whose assignee is idle but never woke up.
//
// Event-driven wakeups (on assignment / on comment) are the fast path.
// They can be silently dropped when the assignee is paused, when the server
// restarts mid-enqueue, or when any future hook is wired incorrectly. This
// reconciler is the slow-path safety net: it converges desired state
// (assignee idle, no run in flight) to actual state by firing wakeups for
// any issue that fell through.
//
// Design notes:
// - Per-agent throttle: skip an agent that already has a queued/running run.
//   Runs execute serially per agent, so one fired wake at a time is enough;
//   the next reconciler tick picks up the next todo when the queue drains.
// - Stale execution lock: an issue whose executionLockedAt is older than
//   STALE_LOCK_MS but never produced an executionRunId is treated as never
//   actually dispatched (covers the prior 409-paused failure mode).
// - Bounded retry: a terminal run should not permanently strand assigned
//   todo work. Failed child work is retried slowly, with a short cooldown and
//   a rolling cap so a broken harness cannot spin forever.

const STALE_LOCK_MS = 5 * 60 * 1000;
const REDISPATCH_COOLDOWN_MS = 30 * 60 * 1000;
const REDISPATCH_GUARD_WINDOW_MS = 12 * 60 * 60 * 1000;
const REDISPATCH_GUARD_RUN_LIMIT = 3;
const TERMINAL_RUN_STATUSES = ["succeeded", "failed", "timed_out", "cancelled"] as const;

export interface DispatchReconcilerHeartbeat {
  wakeup: (
    agentId: string,
    opts: {
      source?: "timer" | "assignment" | "on_demand" | "automation";
      triggerDetail?: "manual" | "ping" | "callback" | "system";
      reason?: string | null;
      payload?: Record<string, unknown> | null;
      requestedByActorType?: "user" | "agent" | "system";
      requestedByActorId?: string | null;
      contextSnapshot?: Record<string, unknown>;
    },
  ) => Promise<unknown>;
}

export interface DispatchReconcilerTickResult {
  considered: number;
  enqueued: number;
  skipped: number;
  guarded: number;
  failed: number;
}

export interface DispatchReconcilerTerminalRun {
  id: string;
  status: string;
  createdAt: Date;
  finishedAt: Date | null;
}

export function shouldGuardRedispatch(
  terminalRuns: DispatchReconcilerTerminalRun[],
  now = new Date(),
) {
  const latestTerminalRun = terminalRuns[0] ?? null;
  if (!latestTerminalRun) {
    return {
      guarded: false,
      reason: null as "cooldown" | "run_limit" | null,
    };
  }

  const latestCompletedAt = latestTerminalRun.finishedAt ?? latestTerminalRun.createdAt;
  if (now.getTime() - latestCompletedAt.getTime() < REDISPATCH_COOLDOWN_MS) {
    return {
      guarded: true,
      reason: "cooldown" as const,
    };
  }

  const guardWindowCutoff = new Date(now.getTime() - REDISPATCH_GUARD_WINDOW_MS);
  const runsInGuardWindow = terminalRuns.filter(
    (run) => (run.finishedAt ?? run.createdAt) >= guardWindowCutoff,
  );
  if (runsInGuardWindow.length >= REDISPATCH_GUARD_RUN_LIMIT) {
    return {
      guarded: true,
      reason: "run_limit" as const,
    };
  }

  return {
    guarded: false,
    reason: null as "cooldown" | "run_limit" | null,
  };
}

export function dispatchReconcilerService(
  db: Db,
  heartbeat: DispatchReconcilerHeartbeat,
) {
  return {
    tick: async (now = new Date()): Promise<DispatchReconcilerTickResult> => {
      const staleCutoff = new Date(now.getTime() - STALE_LOCK_MS);

      const candidates = await db
        .select({
          issueId: issues.id,
          companyId: issues.companyId,
          assigneeAgentId: issues.assigneeAgentId,
        })
        .from(issues)
        .innerJoin(agents, eq(agents.id, issues.assigneeAgentId))
        .where(
          and(
            eq(issues.status, "todo"),
            isNotNull(issues.assigneeAgentId),
            isNull(issues.executionRunId),
            or(
              isNull(issues.executionLockedAt),
              lt(issues.executionLockedAt, staleCutoff),
            ),
            inArray(agents.status, ["idle", "running", "error"]),
          ),
        );

      let enqueued = 0;
      let skipped = 0;
      let guarded = 0;
      let failed = 0;

      // Per-agent throttle: track agents we've already fired for this tick
      // or that already have a queued/running run.
      const blockedAgents = new Set<string>();

      for (const candidate of candidates) {
        const agentId = candidate.assigneeAgentId;
        if (!agentId) continue;

        if (blockedAgents.has(agentId)) {
          skipped += 1;
          continue;
        }

        const [existing] = await db
          .select({ id: heartbeatRuns.id })
          .from(heartbeatRuns)
          .where(
            and(
              eq(heartbeatRuns.agentId, agentId),
              inArray(heartbeatRuns.status, ["queued", "running"]),
            ),
          )
          .limit(1);

        if (existing) {
          blockedAgents.add(agentId);
          skipped += 1;
          continue;
        }

        const recentTerminalRuns = await db
          .select({
            id: heartbeatRuns.id,
            status: heartbeatRuns.status,
            createdAt: heartbeatRuns.createdAt,
            finishedAt: heartbeatRuns.finishedAt,
          })
          .from(heartbeatRuns)
          .where(
            and(
              eq(heartbeatRuns.agentId, agentId),
              inArray(heartbeatRuns.status, [...TERMINAL_RUN_STATUSES]),
              or(
                sql`${heartbeatRuns.contextSnapshot} ->> 'issueId' = ${candidate.issueId}`,
                sql`${heartbeatRuns.contextSnapshot} ->> 'taskId' = ${candidate.issueId}`,
                sql`${heartbeatRuns.contextSnapshot} ->> 'taskKey' = ${candidate.issueId}`,
              ),
            ),
          )
          .orderBy(desc(heartbeatRuns.createdAt))
          .limit(REDISPATCH_GUARD_RUN_LIMIT);

        const redispatchGuard = shouldGuardRedispatch(recentTerminalRuns, now);
        if (redispatchGuard.guarded) {
          guarded += 1;
          logger.warn(
            {
              issueId: candidate.issueId,
              agentId,
              runIds: recentTerminalRuns.map((run) => run.id),
              statuses: recentTerminalRuns.map((run) => run.status),
              reason: redispatchGuard.reason,
            },
            "dispatch reconciler suppressed repeated redispatch",
          );
          continue;
        }

        try {
          await heartbeat.wakeup(agentId, {
            source: "assignment",
            triggerDetail: "system",
            reason: "dispatch_reconciler",
            payload: {
              issueId: candidate.issueId,
              mutation: "dispatch_reconciler",
            },
            requestedByActorType: "system",
            requestedByActorId: "dispatch_reconciler",
            contextSnapshot: {
              issueId: candidate.issueId,
              taskId: candidate.issueId,
              source: "dispatch.reconciler",
              wakeReason: "dispatch_reconciler_recovery",
            },
          });
          enqueued += 1;
          blockedAgents.add(agentId);
        } catch (err) {
          failed += 1;
          logger.warn(
            { err, issueId: candidate.issueId, agentId },
            "dispatch reconciler wakeup failed",
          );
        }
      }

      return {
        considered: candidates.length,
        enqueued,
        skipped,
        guarded,
        failed,
      };
    },
  };
}

export type DispatchReconcilerService = ReturnType<typeof dispatchReconcilerService>;
