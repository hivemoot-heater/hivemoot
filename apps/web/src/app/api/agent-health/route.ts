/**
 * POST / GET  /api/agent-health
 *
 * POST — Accepts health reports from autonomous agents. Authenticated via
 *        Bearer token (agent token). Rate-limited to 1 report/agent/repo/60s.
 *
 * GET  — Returns health overview or per-agent history. Authenticated via
 *        setup session cookie (for dashboard users).
 *        Query params:
 *          (none)                       → overview of all agents
 *          ?agent_id=X&repo=Y           → run history for one agent+repo
 */

import { NextRequest, NextResponse } from "next/server";
import { authenticateByokRequest } from "@/server/byok-auth";
import { authenticateAgentRequest } from "@/server/agent-health-auth";
import {
  validateReport,
  checkRateLimit,
  recordHealthReport,
  getOverview,
  getHistory,
} from "@/server/agent-health-store";
import { AGENT_HEALTH_ERROR, agentHealthError } from "@/server/agent-health-error";

export async function POST(request: NextRequest) {
  const auth = await authenticateAgentRequest(request);
  if (!auth.ok) return auth.response;

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return agentHealthError(
      AGENT_HEALTH_ERROR.INVALID_JSON,
      "Invalid JSON body",
      400,
    );
  }

  const validation = validateReport(body);
  if (!validation.ok) {
    return agentHealthError(
      AGENT_HEALTH_ERROR.VALIDATION_FAILED,
      validation.message,
      400,
    );
  }

  const { report } = validation;
  const allowed = await checkRateLimit(
    auth.installationId,
    report.agent_id,
    report.repo,
    auth.redis,
  );

  if (!allowed) {
    return agentHealthError(
      AGENT_HEALTH_ERROR.RATE_LIMITED,
      "Rate limited — one report per agent per repo per 60 seconds",
      429,
    );
  }

  await recordHealthReport(auth.installationId, report, auth.redis);

  return NextResponse.json({ received: true, received_at: report.received_at });
}

export async function GET(request: NextRequest) {
  const auth = await authenticateByokRequest(request);
  if (!auth.ok) return auth.response;

  const { searchParams } = new URL(request.url);
  const agentId = searchParams.get("agent_id");
  const repo = searchParams.get("repo");

  // If both agent_id and repo are provided, return per-agent history
  if (agentId && repo) {
    const history = await getHistory(
      auth.session.installationId,
      agentId,
      repo,
      auth.redis,
    );
    return NextResponse.json({ agent_id: agentId, repo, history });
  }

  // If only one is provided, that's a bad request
  if (agentId || repo) {
    return agentHealthError(
      AGENT_HEALTH_ERROR.MISSING_FIELDS,
      "Both agent_id and repo are required for history queries",
      400,
    );
  }

  // Default: overview of all agents
  const overview = await getOverview(auth.session.installationId, auth.redis);
  return NextResponse.json({ agents: overview });
}
