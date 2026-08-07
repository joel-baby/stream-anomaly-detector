import { redis } from "./redisClient.js";
import { STREAM_KEY, GROUP_NAME, CONSUMER_NAME } from "./config.js";

export async function ensureGroupExists() {
  try {
    await redis.xgroup(STREAM_KEY, {
      type: "CREATE",
      group: GROUP_NAME,
      id: "$",
      options: { MKSTREAM: true },
    });
    console.log(`Created consumer group "${GROUP_NAME}"`);
  } catch (err: any) {
    if (err.message?.includes("BUSYGROUP")) {
      console.log(`Consumer group "${GROUP_NAME}" already exists, continuing.`);
    } else {
      throw err;
    }
  }
}

export function fieldsToObject(fields: string[]): Record<string, string> {
  const obj: Record<string, string> = {};
  for (let i = 0; i < fields.length; i += 2) {
    obj[fields[i]] = fields[i + 1];
  }
  return obj;
}

// Polls for new events (no true blocking on Upstash's REST API).
export async function readEvents() {
  return (await redis.xreadgroup(
    GROUP_NAME,
    CONSUMER_NAME,
    STREAM_KEY,
    ">",
    { count: 10 }
  )) as [string, [string, string[]][]][] | null;
}

export async function acknowledge(eventId: string) {
  await redis.xack(STREAM_KEY, GROUP_NAME, eventId);
}