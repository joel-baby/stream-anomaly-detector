import { Redis } from "@upstash/redis";
import "dotenv/config";

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!,
});

const PREFIX = process.env.REDIS_KEY_PREFIX ?? "stream-anomaly";
const STREAM_KEY = `${PREFIX}:transactions`;
const GROUP_NAME = `${PREFIX}:consumer-group`;
const CONSUMER_NAME = "consumer-1"; // unique per consumer if you ever run more than one

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

          console.log(
            `[consumed] id=${id} user=${userId} amount=$${amount}  ` +
              `| window(5m): count=${stats.count} totalSpend=$${stats.totalSpend.toFixed(2)}`,
          );

          await redis.xack(STREAM_KEY, GROUP_NAME, id);
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

main();
