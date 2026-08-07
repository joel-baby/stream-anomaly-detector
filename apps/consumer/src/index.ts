import { Redis } from "@upstash/redis";
import "dotenv/config";
import { MongoClient, Collection } from "mongodb";

const mongoClient = new MongoClient(process.env.MONGODB_URI!);
let flaggedEventsCollection: Collection;

async function connectMongo() {
  await mongoClient.connect();
  const db = mongoClient.db(process.env.MONGODB_DB_NAME ?? "stream-anomaly");
  flaggedEventsCollection = db.collection("flagged_events");
  console.log("Connected to MongoDB");
}

type FlaggedEvent = {
  userId: string;
  eventId: string;
  amount: number;
  timestamp: number;
  windowCount: number;
  windowSpend: number;
  baselineAvgCount: number;
  baselineAvgSpend: number;
  flaggedAt: Date;
};

async function persistFlaggedEvent(event: FlaggedEvent) {
  try {
    await flaggedEventsCollection.insertOne(event);
  } catch (err) {
    console.error("Failed to persist flagged event to MongoDB:", err);
    // Deliberately not re-throwing: a Mongo write failure shouldn't crash
    // the whole consumer or block acknowledging the Redis event. We'll
    // revisit this tradeoff in Stage 8 (resilience).
  }
}

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!,
});

const PREFIX = process.env.REDIS_KEY_PREFIX ?? "stream-anomaly";
const STREAM_KEY = `${PREFIX}:transactions`;
const GROUP_NAME = `${PREFIX}:consumer-group`;
const CONSUMER_NAME = "consumer-1"; // unique per consumer if you ever run more than one

const BASELINE_UPDATE_INTERVAL_MS = parseInt(
  process.env.BASELINE_UPDATE_INTERVAL_MS ?? "60000",
  10,
);

function baselineTimestampKey(userId: string): string {
  return `${PREFIX}:baseline-updated:${userId}`;
}

// Returns true if enough time has passed since we last updated this
// user's baseline — this throttles updates so a burst can't teach the
// baseline to treat itself as normal while it's still happening.
async function shouldUpdateBaseline(
  userId: string,
  nowMs: number,
): Promise<boolean> {
  const lastUpdate = await redis.get<number>(baselineTimestampKey(userId));
  if (lastUpdate === null) return true;
  return nowMs - lastUpdate >= BASELINE_UPDATE_INTERVAL_MS;
}

async function markBaselineUpdated(userId: string, nowMs: number) {
  await redis.set(baselineTimestampKey(userId), nowMs);
}

// One-time setup: create the consumer group if it doesn't already exist.
// "$" = start from new events only, ignore anything already in the stream.
// MKSTREAM = create the stream itself if it doesn't exist yet.
async function ensureGroupExists() {
  try {
    await redis.xgroup(STREAM_KEY, {
      type: "CREATE",
      group: GROUP_NAME,
      id: "$",
      options: { MKSTREAM: true },
    });
    console.log(`Created consumer group "${GROUP_NAME}"`);
  } catch (err: any) {
    // Redis errors if the group already exists — fine, just means a
    // previous run already set this up.
    if (err.message?.includes("BUSYGROUP")) {
      console.log(`Consumer group "${GROUP_NAME}" already exists, continuing.`);
    } else {
      throw err;
    }
  }
}

// Converts Redis's flat [field, value, field, value, ...] array
// into a normal { field: value } object, easier to work with.
function fieldsToObject(fields: string[]): Record<string, string> {
  const obj: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) {
    obj[fields[i]] = fields[i + 1];
  }
  return obj;
}

async function main() {
  await connectMongo();
  await ensureGroupExists();
  console.log(
    `Consumer "${CONSUMER_NAME}" started. Listening on: ${STREAM_KEY}\n`,
  );

  while (true) {
    try {
      // Upstash's REST API has no true blocking (no persistent connection
      // to hold open), so we poll: read what's available, and if nothing
      // is there, wait briefly before asking again.
      const result = (await redis.xreadgroup(
        GROUP_NAME,
        CONSUMER_NAME,
        STREAM_KEY,
        ">",
        { count: 10 },
      )) as [string, [string, string[]][]][] | null;

      if (!result) {
        await new Promise((r) => setTimeout(r, 500)); // nothing new, brief pause
        continue;
      }

      for (const [, messages] of result) {
        for (const [id, fields] of messages) {
          const { userId, amount, timestamp } = fieldsToObject(fields);

          const ts = parseInt(timestamp, 10);
          const amt = parseFloat(amount);

          await recordInWindow(userId, amt, ts);
          const stats = await getWindowStats(userId, ts);
          const baseline = await getBaseline(userId);

          const flagged = isAnomalous(stats.count, stats.totalSpend, baseline);

          if (flagged) {
            console.log(
              `🚨 [ANOMALY] user=${userId} count=${stats.count} (baseline avg=${baseline.avgCount.toFixed(1)}) ` +
                `spend=$${stats.totalSpend.toFixed(2)} (baseline avg=$${baseline.avgSpend.toFixed(2)})`,
            );

            await persistFlaggedEvent({
              userId,
              eventId: id,
              amount: amt,
              timestamp: ts,
              windowCount: stats.count,
              windowSpend: stats.totalSpend,
              baselineAvgCount: baseline.avgCount,
              baselineAvgSpend: baseline.avgSpend,
              flaggedAt: new Date(),
            });
          } else {
            console.log(
              `[consumed] id=${id} user=${userId} amount=$${amount} ` +
                `| window(5m): count=${stats.count} totalSpend=$${stats.totalSpend.toFixed(2)}`,
            );
          }

          // Only fold this window into the baseline if it's NOT flagged, and only
          // if enough time has passed since the last update. This is what stops
          // a burst from redefining itself as "normal" while it's still ongoing.
          if (!flagged && (await shouldUpdateBaseline(userId, ts))) {
            await updateBaseline(userId, stats.count, stats.totalSpend);
            await markBaselineUpdated(userId, ts);
          }
        }
      }
    } catch (err) {
      console.error("Error reading from stream:", err);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

const WINDOW_MS = 5 * 60 * 1000; // 5 minutes

function windowKey(userId: string): string {
  return `${PREFIX}:window:${userId}`;
}

// Records this event in the user's rolling window, then trims anything
// older than WINDOW_MS out of the set — this is the "aging out" step.
async function recordInWindow(
  userId: string,
  amount: number,
  timestampMs: number,
) {
  const key = windowKey(userId);

  // Store amount as the member (so we can sum it later), timestamp as the score.
  // We tack the event id-ish suffix on so repeated identical amounts don't collide
  // as the same ZSET member.
  const member = `${amount}:${timestampMs}:${Math.random().toString(36).slice(2, 7)}`;

  await redis.zadd(key, { score: timestampMs, member });

  // Remove anything older than the window from this user's set.
  const cutoff = timestampMs - WINDOW_MS;
  await redis.zremrangebyscore(key, 0, cutoff);

  // Let this key expire on its own if the user goes quiet, so we don't
  // keep empty/stale keys around forever.
  await redis.expire(key, Math.ceil(WINDOW_MS / 1000) * 2);
}

// Returns { count, totalSpend } for everything currently in this user's window.
async function getWindowStats(userId: string, timestampMs: number) {
  const key = windowKey(userId);
  const cutoff = timestampMs - WINDOW_MS;

  // Get all members currently in the window (score > cutoff).
  const members = (await redis.zrange(key, cutoff, "+inf", {
    byScore: true,
  })) as string[];

  let totalSpend = 0;
  for (const member of members) {
    const [amountStr] = member.split(":");
    totalSpend += parseFloat(amountStr);
  }

  return { count: members.length, totalSpend };
}

const ANOMALY_MULTIPLIER = parseFloat(process.env.ANOMALY_MULTIPLIER ?? "4");
const EWMA_ALPHA = parseFloat(process.env.EWMA_ALPHA ?? "0.2");

function baselineKey(userId: string): string {
  return `${PREFIX}:baseline:${userId}`;
}

type Baseline = {
  avgCount: number;
  avgSpend: number;
  samples: number; // how many windows we've factored in so far
};

// Fetches the user's current baseline, or a sensible empty default
// if they've never been seen before.
async function getBaseline(userId: string): Promise<Baseline> {
  const raw = await redis.get<Baseline>(baselineKey(userId));
  return raw ?? { avgCount: 0, avgSpend: 0, samples: 0 };
}

// Nudges the baseline toward the current window's values using EWMA:
// newAvg = alpha * currentValue + (1 - alpha) * oldAvg
// In plain terms: blend the new observation into the running average,
// weighted so recent activity matters but doesn't overwrite history.
async function updateBaseline(
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

// Decides whether the current window looks anomalous relative to baseline.
// Requires a minimum number of samples so we're not flagging brand-new
// users just because we have no history yet (that would be a cheap,
// meaningless "anomaly").
const MIN_SAMPLES_BEFORE_SCORING = 3;

function isAnomalous(
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

main();
