import { redis } from "./redisClient.js";
import {
  PREFIX,
  ANOMALY_MULTIPLIER,
  EWMA_ALPHA,
  MIN_SAMPLES_BEFORE_SCORING,
  BASELINE_UPDATE_INTERVAL_MS,
} from "./config.js";

export type Baseline = {
  avgCount: number;
  avgSpend: number;
  samples: number;
};

function baselineKey(userId: string): string {
  return `${PREFIX}:baseline:${userId}`;
}

function baselineTimestampKey(userId: string): string {
  return `${PREFIX}:baseline-updated:${userId}`;
}

export async function getBaseline(userId: string): Promise<Baseline> {
  const raw = await redis.get<Baseline>(baselineKey(userId));
  return raw ?? { avgCount: 0, avgSpend: 0, samples: 0 };
}

// Nudges the baseline toward the current window's values using EWMA.
export async function updateBaseline(
  userId: string,
  currentCount: number,
  currentSpend: number,
) {
  const baseline = await getBaseline(userId);

  const updated: Baseline = {
    avgCount:
      baseline.samples === 0
        ? currentCount
        : EWMA_ALPHA * currentCount + (1 - EWMA_ALPHA) * baseline.avgCount,
    avgSpend:
      baseline.samples === 0
        ? currentSpend
        : EWMA_ALPHA * currentSpend + (1 - EWMA_ALPHA) * baseline.avgSpend,
    samples: baseline.samples + 1,
  };

  await redis.set(baselineKey(userId), updated);
  return updated;
}

// Throttling: only update baseline periodically, not on every event,
// so a burst can't teach the baseline to see itself as normal.
export async function shouldUpdateBaseline(
  userId: string,
  nowMs: number,
): Promise<boolean> {
  const lastUpdate = await redis.get<number>(baselineTimestampKey(userId));
  if (lastUpdate === null) return true;
  return nowMs - lastUpdate >= BASELINE_UPDATE_INTERVAL_MS;
}

export async function markBaselineUpdated(userId: string, nowMs: number) {
  await redis.set(baselineTimestampKey(userId), nowMs);
}

export function isAnomalous(
  currentCount: number,
  currentSpend: number,
  baseline: Baseline,
): boolean {
  if (baseline.samples < MIN_SAMPLES_BEFORE_SCORING) return false;
  if (baseline.avgCount === 0) return false;

  const countRatio = currentCount / baseline.avgCount;
  const spendRatio =
    baseline.avgSpend > 0 ? currentSpend / baseline.avgSpend : 0;

  return countRatio >= ANOMALY_MULTIPLIER || spendRatio >= ANOMALY_MULTIPLIER;
}
