import { describe, it, expect, vi, beforeEach } from "vitest";
import { type Redis } from "@upstash/redis";
import {
  validateReport,
  checkRateLimit,
  recordHealthReport,
  getOverview,
  getHistory,
  type HealthReport,
} from "./agent-health-store";

// ---------------------------------------------------------------------------
// Minimal Redis mock with sorted set support
// ---------------------------------------------------------------------------

function makeMockRedis() {
  const store = new Map<string, unknown>();
  const sets = new Map<string, Set<string>>();
  const sortedSets = new Map<string, Map<string, number>>(); // member → score

  const client = {
    set: vi.fn(async (key: string, value: unknown, opts?: { nx?: boolean; ex?: number }) => {
      if (opts?.nx && store.has(key)) return null;
      store.set(key, value);
      return "OK";
    }),
    get: vi.fn(async (key: string) => store.get(key) ?? null),
    del: vi.fn(async (key: string) => {
      const existed = store.has(key);
      store.delete(key);
      return existed ? 1 : 0;
    }),
    ttl: vi.fn(async (key: string) => {
      return store.has(key) ? 1800 : -2;
    }),
    sadd: vi.fn(async (key: string, ...members: string[]) => {
      if (!sets.has(key)) sets.set(key, new Set());
      const set = sets.get(key)!;
      let added = 0;
      for (const m of members) {
        if (!set.has(m)) { set.add(m); added++; }
      }
      return added;
    }),
    smembers: vi.fn(async (key: string) => {
      const set = sets.get(key);
      return set ? [...set] : [];
    }),
    zadd: vi.fn(async (key: string, entry: { score: number; member: string }) => {
      if (!sortedSets.has(key)) sortedSets.set(key, new Map());
      sortedSets.get(key)!.set(entry.member, entry.score);
      return 1;
    }),
    zrange: vi.fn(async (key: string, start: number, stop: number, opts?: { rev?: boolean }) => {
      const zset = sortedSets.get(key);
      if (!zset) return [];
      const entries = [...zset.entries()].sort((a, b) =>
        opts?.rev ? b[1] - a[1] : a[1] - b[1],
      );
      return entries.slice(start, stop + 1).map(([member]) => member);
    }),
    zremrangebyscore: vi.fn(async (key: string, min: string | number, max: string | number) => {
      const zset = sortedSets.get(key);
      if (!zset) return 0;
      const minScore = min === "-inf" ? -Infinity : Number(min);
      const maxScore = max === "+inf" ? Infinity : Number(max);
      let removed = 0;
      for (const [member, score] of zset) {
        if (score >= minScore && score <= maxScore) {
          zset.delete(member);
          removed++;
        }
      }
      return removed;
    }),
    _store: store,
    _sets: sets,
    _sortedSets: sortedSets,
  };
  return client as unknown as Redis & {
    _store: Map<string, unknown>;
    _sets: Map<string, Set<string>>;
    _sortedSets: Map<string, Map<string, number>>;
  };
}

// ---------------------------------------------------------------------------
// Tests — validateReport
// ---------------------------------------------------------------------------

describe("validateReport", () => {
  it("accepts a valid minimal report", () => {
    const result = validateReport({
      agent_id: "bee-1",
      repo: "hivemoot/sandbox",
      status: "idle",
    });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.report.agent_id).toBe("bee-1");
      expect(result.report.repo).toBe("hivemoot/sandbox");
      expect(result.report.status).toBe("idle");
      expect(result.report.received_at).toBeDefined();
    }
  });

  it("accepts a full report with optional fields", () => {
    const result = validateReport({
      agent_id: "bee-1",
      repo: "hivemoot/sandbox",
      status: "working",
      current_issue: 42,
      summary: "Implementing feature X",
    });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.report.current_issue).toBe(42);
      expect(result.report.summary).toBe("Implementing feature X");
    }
  });

  it("accepts error status with error_message", () => {
    const result = validateReport({
      agent_id: "bee-1",
      repo: "hivemoot/sandbox",
      status: "error",
      error_message: "Build failed",
    });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.report.error_message).toBe("Build failed");
    }
  });

  it("rejects non-object body", () => {
    expect(validateReport("string").ok).toBe(false);
    expect(validateReport(null).ok).toBe(false);
    expect(validateReport([]).ok).toBe(false);
  });

  it("rejects missing agent_id", () => {
    const result = validateReport({ repo: "r", status: "idle" });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.message).toContain("agent_id");
  });

  it("rejects empty agent_id", () => {
    const result = validateReport({ agent_id: "", repo: "r", status: "idle" });
    expect(result.ok).toBe(false);
  });

  it("rejects missing repo", () => {
    const result = validateReport({ agent_id: "a", status: "idle" });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.message).toContain("repo");
  });

  it("rejects invalid status", () => {
    const result = validateReport({ agent_id: "a", repo: "r", status: "unknown" });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.message).toContain("status");
  });

  it("rejects non-number current_issue", () => {
    const result = validateReport({
      agent_id: "a",
      repo: "r",
      status: "idle",
      current_issue: "not-a-number",
    });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.message).toContain("current_issue");
  });

  it("server-assigns received_at, ignoring client value", () => {
    const result = validateReport({
      agent_id: "bee-1",
      repo: "r",
      status: "idle",
      received_at: "2020-01-01T00:00:00Z",
    });
    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.report.received_at).not.toBe("2020-01-01T00:00:00Z");
    }
  });
});

// ---------------------------------------------------------------------------
// Tests — checkRateLimit
// ---------------------------------------------------------------------------

describe("checkRateLimit", () => {
  let redis: ReturnType<typeof makeMockRedis>;

  beforeEach(() => {
    vi.clearAllMocks();
    redis = makeMockRedis();
  });

  it("allows the first request", async () => {
    const allowed = await checkRateLimit("inst-1", "bee-1", "repo", redis);
    expect(allowed).toBe(true);
  });

  it("blocks a second request within the window", async () => {
    await checkRateLimit("inst-1", "bee-1", "repo", redis);
    const allowed = await checkRateLimit("inst-1", "bee-1", "repo", redis);
    expect(allowed).toBe(false);
  });

  it("allows requests from different agents", async () => {
    await checkRateLimit("inst-1", "bee-1", "repo", redis);
    const allowed = await checkRateLimit("inst-1", "bee-2", "repo", redis);
    expect(allowed).toBe(true);
  });

  it("allows requests for different repos", async () => {
    await checkRateLimit("inst-1", "bee-1", "repo-a", redis);
    const allowed = await checkRateLimit("inst-1", "bee-1", "repo-b", redis);
    expect(allowed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Tests — recordHealthReport
// ---------------------------------------------------------------------------

describe("recordHealthReport", () => {
  let redis: ReturnType<typeof makeMockRedis>;

  beforeEach(() => {
    vi.clearAllMocks();
    redis = makeMockRedis();
  });

  it("stores the latest report with TTL", async () => {
    const report: HealthReport = {
      agent_id: "bee-1",
      repo: "hivemoot/sandbox",
      status: "idle",
      received_at: "2026-02-24T10:00:00Z",
    };

    await recordHealthReport("inst-1", report, redis);

    // Check SET was called with ex option
    expect(redis.set).toHaveBeenCalledWith(
      "agent-health:latest:inst-1:bee-1:hivemoot/sandbox",
      report,
      { ex: 1800 },
    );
  });

  it("adds to the runs sorted set", async () => {
    const report: HealthReport = {
      agent_id: "bee-1",
      repo: "hivemoot/sandbox",
      status: "working",
      received_at: "2026-02-24T10:00:00Z",
    };

    await recordHealthReport("inst-1", report, redis);

    expect(redis.zadd).toHaveBeenCalledWith(
      "agent-health:runs:inst-1:bee-1:hivemoot/sandbox",
      {
        score: new Date("2026-02-24T10:00:00Z").getTime(),
        member: JSON.stringify(report),
      },
    );
  });

  it("adds agent:repo to the index set", async () => {
    const report: HealthReport = {
      agent_id: "bee-1",
      repo: "hivemoot/sandbox",
      status: "idle",
      received_at: "2026-02-24T10:00:00Z",
    };

    await recordHealthReport("inst-1", report, redis);

    expect(redis.sadd).toHaveBeenCalledWith(
      "agent-health:index:inst-1",
      "bee-1:hivemoot/sandbox",
    );
  });

  it("trims old entries from runs", async () => {
    const report: HealthReport = {
      agent_id: "bee-1",
      repo: "hivemoot/sandbox",
      status: "idle",
      received_at: "2026-02-24T10:00:00Z",
    };

    await recordHealthReport("inst-1", report, redis);

    const receivedMs = new Date("2026-02-24T10:00:00Z").getTime();
    const cutoff = receivedMs - 24 * 60 * 60 * 1000;

    expect(redis.zremrangebyscore).toHaveBeenCalledWith(
      "agent-health:runs:inst-1:bee-1:hivemoot/sandbox",
      "-inf",
      cutoff,
    );
  });
});

// ---------------------------------------------------------------------------
// Tests — getOverview
// ---------------------------------------------------------------------------

describe("getOverview", () => {
  let redis: ReturnType<typeof makeMockRedis>;

  beforeEach(() => {
    vi.clearAllMocks();
    redis = makeMockRedis();
  });

  it("returns empty array when no agents are indexed", async () => {
    const result = await getOverview("inst-1", redis);
    expect(result).toEqual([]);
  });

  it("returns overview entries for indexed agents", async () => {
    const report: HealthReport = {
      agent_id: "bee-1",
      repo: "hivemoot/sandbox",
      status: "working",
      current_issue: 42,
      received_at: "2026-02-24T10:00:00Z",
    };

    // Simulate state after recordHealthReport
    redis._sets.set("agent-health:index:inst-1", new Set(["bee-1:hivemoot/sandbox"]));
    redis._store.set("agent-health:latest:inst-1:bee-1:hivemoot/sandbox", report);

    const result = await getOverview("inst-1", redis);
    expect(result).toHaveLength(1);
    expect(result[0].agent_id).toBe("bee-1");
    expect(result[0].repo).toBe("hivemoot/sandbox");
    expect(result[0].status).toBe("working");
    expect(result[0].current_issue).toBe(42);
    expect(result[0].online).toBe(true);
  });

  it("marks agents as offline when latest key has expired", async () => {
    redis._sets.set("agent-health:index:inst-1", new Set(["bee-1:hivemoot/sandbox"]));
    // No latest key stored → expired
    vi.mocked(redis.ttl).mockResolvedValue(-2);

    const result = await getOverview("inst-1", redis);
    expect(result).toHaveLength(1);
    expect(result[0].online).toBe(false);
    expect(result[0].agent_id).toBe("bee-1");
  });

  it("sorts entries by received_at descending", async () => {
    redis._sets.set("agent-health:index:inst-1", new Set([
      "bee-1:hivemoot/sandbox",
      "bee-2:hivemoot/sandbox",
    ]));
    redis._store.set("agent-health:latest:inst-1:bee-1:hivemoot/sandbox", {
      agent_id: "bee-1",
      repo: "hivemoot/sandbox",
      status: "idle",
      received_at: "2026-02-24T09:00:00Z",
    });
    redis._store.set("agent-health:latest:inst-1:bee-2:hivemoot/sandbox", {
      agent_id: "bee-2",
      repo: "hivemoot/sandbox",
      status: "working",
      received_at: "2026-02-24T10:00:00Z",
    });

    const result = await getOverview("inst-1", redis);
    expect(result[0].agent_id).toBe("bee-2"); // more recent
    expect(result[1].agent_id).toBe("bee-1");
  });
});

// ---------------------------------------------------------------------------
// Tests — getHistory
// ---------------------------------------------------------------------------

describe("getHistory", () => {
  let redis: ReturnType<typeof makeMockRedis>;

  beforeEach(() => {
    vi.clearAllMocks();
    redis = makeMockRedis();
  });

  it("returns empty array when no history exists", async () => {
    const result = await getHistory("inst-1", "bee-1", "repo", redis);
    expect(result).toEqual([]);
  });

  it("returns parsed reports from sorted set", async () => {
    const recentTimestamp = new Date(Date.now() - 60_000).toISOString(); // 1 min ago
    const report: HealthReport = {
      agent_id: "bee-1",
      repo: "hivemoot/sandbox",
      status: "idle",
      received_at: recentTimestamp,
    };

    // Simulate stored sorted set data
    const key = "agent-health:runs:inst-1:bee-1:hivemoot/sandbox";
    redis._sortedSets.set(key, new Map([
      [JSON.stringify(report), new Date(recentTimestamp).getTime()],
    ]));

    const result = await getHistory("inst-1", "bee-1", "hivemoot/sandbox", redis);
    expect(result).toHaveLength(1);
    expect(result[0].agent_id).toBe("bee-1");
    expect(result[0].status).toBe("idle");
  });

  it("trims stale entries before returning", async () => {
    await getHistory("inst-1", "bee-1", "repo", redis);
    expect(redis.zremrangebyscore).toHaveBeenCalled();
  });
});
