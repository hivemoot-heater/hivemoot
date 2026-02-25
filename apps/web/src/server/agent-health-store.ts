/**
 * Agent health report storage and retrieval.
 *
 * Redis layout per agent:
 *
 *   agent-health:latest:{installId}:{agentId}:{repo}
 *     → HealthReport JSON, TTL 30 min (online indicator)
 *
 *   agent-health:runs:{installId}:{agentId}:{repo}
 *     → Sorted set, score = received_at epoch ms, member = JSON report
 *     → Trimmed to last 24 hours on each write
 *
 *   agent-health:index:{installId}
 *     → Set of "{agentId}:{repo}" combos for enumeration
 *
 *   agent-health:ratelimit:{installId}:{agentId}:{repo}
 *     → NX/EX guard — one report per agent per repo per 60 seconds
 */

import { type Redis } from "@upstash/redis";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LATEST_TTL_SECONDS = 30 * 60; // 30 minutes
const RATE_LIMIT_SECONDS = 60;
const HISTORY_RETENTION_MS = 24 * 60 * 60 * 1000; // 24 hours
const MAX_HISTORY_ENTRIES = 1440; // 24h at 1/min

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface HealthReport {
  agent_id: string;
  repo: string;
  status: "idle" | "working" | "error";
  current_issue?: number;
  summary?: string;
  error_message?: string;
  received_at: string; // ISO 8601, server-assigned
}

export interface HealthOverviewEntry {
  agent_id: string;
  repo: string;
  status: HealthReport["status"];
  current_issue?: number;
  summary?: string;
  error_message?: string;
  received_at: string;
  online: boolean; // true if latest key still has TTL remaining
}

// ---------------------------------------------------------------------------
// Key builders
// ---------------------------------------------------------------------------

function latestKey(installId: string, agentId: string, repo: string): string {
  return `agent-health:latest:${installId}:${agentId}:${repo}`;
}

function runsKey(installId: string, agentId: string, repo: string): string {
  return `agent-health:runs:${installId}:${agentId}:${repo}`;
}

function indexKey(installId: string): string {
  return `agent-health:index:${installId}`;
}

function rateLimitKey(installId: string, agentId: string, repo: string): string {
  return `agent-health:ratelimit:${installId}:${agentId}:${repo}`;
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

const VALID_STATUSES = new Set(["idle", "working", "error"]);

export type ValidationResult = {
  ok: true;
  report: HealthReport;
} | {
  ok: false;
  message: string;
};

export function validateReport(body: unknown): ValidationResult {
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    return { ok: false, message: "Body must be a JSON object" };
  }

  const obj = body as Record<string, unknown>;

  if (typeof obj.agent_id !== "string" || obj.agent_id.length === 0) {
    return { ok: false, message: "agent_id is required and must be a non-empty string" };
  }
  if (typeof obj.repo !== "string" || obj.repo.length === 0) {
    return { ok: false, message: "repo is required and must be a non-empty string" };
  }
  if (typeof obj.status !== "string" || !VALID_STATUSES.has(obj.status)) {
    return { ok: false, message: "status must be one of: idle, working, error" };
  }

  if (obj.current_issue !== undefined && typeof obj.current_issue !== "number") {
    return { ok: false, message: "current_issue must be a number if provided" };
  }
  if (obj.summary !== undefined && typeof obj.summary !== "string") {
    return { ok: false, message: "summary must be a string if provided" };
  }
  if (obj.error_message !== undefined && typeof obj.error_message !== "string") {
    return { ok: false, message: "error_message must be a string if provided" };
  }

  const report: HealthReport = {
    agent_id: obj.agent_id,
    repo: obj.repo,
    status: obj.status as HealthReport["status"],
    received_at: new Date().toISOString(),
  };

  if (typeof obj.current_issue === "number") report.current_issue = obj.current_issue;
  if (typeof obj.summary === "string") report.summary = obj.summary;
  if (typeof obj.error_message === "string") report.error_message = obj.error_message;

  return { ok: true, report };
}

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

/**
 * Returns true if the request is allowed, false if rate-limited.
 * Uses SET NX EX for atomic check-and-set with automatic expiry.
 */
export async function checkRateLimit(
  installId: string,
  agentId: string,
  repo: string,
  redis: Redis,
): Promise<boolean> {
  const result = await redis.set(
    rateLimitKey(installId, agentId, repo),
    "1",
    { nx: true, ex: RATE_LIMIT_SECONDS },
  );
  // Upstash returns "OK" on success, null if key already exists
  return result === "OK";
}

// ---------------------------------------------------------------------------
// Write pipeline
// ---------------------------------------------------------------------------

/**
 * Records a validated health report in Redis.
 * Pipeline: SET latest (30min TTL) + ZADD runs + SADD index + trim old runs.
 */
export async function recordHealthReport(
  installId: string,
  report: HealthReport,
  redis: Redis,
): Promise<void> {
  const { agent_id, repo, received_at } = report;
  const score = new Date(received_at).getTime();

  // SET latest with TTL
  await redis.set(
    latestKey(installId, agent_id, repo),
    report,
    { ex: LATEST_TTL_SECONDS },
  );

  // ZADD to sorted set (score = epoch ms)
  await redis.zadd(
    runsKey(installId, agent_id, repo),
    { score, member: JSON.stringify(report) },
  );

  // SADD to index
  await redis.sadd(indexKey(installId), `${agent_id}:${repo}`);

  // Trim runs older than 24 hours
  const cutoff = score - HISTORY_RETENTION_MS;
  await redis.zremrangebyscore(
    runsKey(installId, agent_id, repo),
    "-inf",
    cutoff,
  );
}

// ---------------------------------------------------------------------------
// Read functions (used by GET endpoint in PR 3)
// ---------------------------------------------------------------------------

/**
 * Returns an overview of all agents for an installation.
 * One entry per agent+repo combo, with online status derived from TTL.
 */
export async function getOverview(
  installId: string,
  redis: Redis,
): Promise<HealthOverviewEntry[]> {
  const members = await redis.smembers(indexKey(installId));
  if (!members || members.length === 0) return [];

  const entries: HealthOverviewEntry[] = [];

  for (const member of members) {
    const separatorIdx = member.indexOf(":");
    if (separatorIdx === -1) continue;

    const agentId = member.slice(0, separatorIdx);
    const repo = member.slice(separatorIdx + 1);
    const key = latestKey(installId, agentId, repo);

    const report = await redis.get<HealthReport>(key);
    const ttl = await redis.ttl(key);
    const online = ttl > 0;

    if (report && typeof report.agent_id === "string") {
      entries.push({
        agent_id: report.agent_id,
        repo: report.repo,
        status: report.status,
        current_issue: report.current_issue,
        summary: report.summary,
        error_message: report.error_message,
        received_at: report.received_at,
        online,
      });
    } else {
      // Agent existed but latest key expired — show as offline
      entries.push({
        agent_id: agentId,
        repo,
        status: "idle",
        received_at: "",
        online: false,
      });
    }
  }

  // Sort by received_at descending (most recent first)
  entries.sort((a, b) => {
    if (!a.received_at) return 1;
    if (!b.received_at) return -1;
    return b.received_at.localeCompare(a.received_at);
  });

  return entries;
}

/**
 * Returns the run history for a specific agent+repo combo.
 * Results are sorted newest-first, limited to MAX_HISTORY_ENTRIES.
 */
export async function getHistory(
  installId: string,
  agentId: string,
  repo: string,
  redis: Redis,
): Promise<HealthReport[]> {
  const key = runsKey(installId, agentId, repo);

  // Trim stale entries first
  const now = Date.now();
  const cutoff = now - HISTORY_RETENTION_MS;
  await redis.zremrangebyscore(key, "-inf", cutoff);

  // Fetch newest-first
  const raw = await redis.zrange(key, 0, MAX_HISTORY_ENTRIES - 1, { rev: true });
  if (!raw || raw.length === 0) return [];

  return raw
    .map((entry) => {
      if (typeof entry === "string") {
        try { return JSON.parse(entry) as HealthReport; } catch { return null; }
      }
      return entry as HealthReport;
    })
    .filter((r): r is HealthReport => r !== null);
}
