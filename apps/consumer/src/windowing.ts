import { redis } from "./redisClient.js";
import { PREFIX, WINDOW_MS } from "./config.js";

function windowKey(userId: string): string {
  return `${PREFIX}:window:${userId}`;
}

// Records this event in the user's rolling window, then trims anything
// older than WINDOW_MS out of the set.
export async function recordInWindow(userId: string, amount: number, timestampMs: number) {
  const key = windowKey(userId);
  const member = `${amount}:${timestampMs}:${Math.random().toString(36).slice(2, 7)}`;

  await redis.zadd(key, { score: timestampMs, member });

  const cutoff = timestampMs - WINDOW_MS;
  await redis.zremrangebyscore(key, 0, cutoff);
  await redis.expire(key, Math.ceil(WINDOW_MS / 1000) * 2);
}

// Returns { count, totalSpend } for everything currently in this user's window.
export async function getWindowStats(userId: string, timestampMs: number) {
  const key = windowKey(userId);
  const cutoff = timestampMs - WINDOW_MS;

  const members = await redis.zrange(key, cutoff, "+inf", { byScore: true }) as string[];

  let totalSpend = 0;
  for (const member of members) {
    const [amountStr] = member.split(":");
    totalSpend += parseFloat(amountStr);
  }

  return { count: members.length, totalSpend };
}