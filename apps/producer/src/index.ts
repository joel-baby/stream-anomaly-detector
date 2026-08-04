import { Redis } from "@upstash/redis";
import "dotenv/config";

// Connect to your shared Upstash Redis using the credentials from .env
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL!,
  token: process.env.UPSTASH_REDIS_REST_TOKEN!,
});

// Everything this project touches in Redis is prefixed, so it never
// collides with your project 1 data.
const PREFIX = process.env.REDIS_KEY_PREFIX ?? "stream-anomaly";
const STREAM_KEY = `${PREFIX}:transactions`;

// A small fixed pool of fake users, so the same user can show
// repeated activity (needed later for windowing/anomaly detection).
const USER_IDS = Array.from({ length: 20 }, (_, i) => `user_${i + 1}`);

function randomUser(): string {
  return USER_IDS[Math.floor(Math.random() * USER_IDS.length)];
}

function randomAmount(): number {
  // Normal purchase: $5 - $200
  return Math.round((Math.random() * 195 + 5) * 100) / 100;
}

// Pushes one event onto the Redis Stream.
async function emitEvent(userId: string, amount: number) {
  const event = {
    userId,
    amount: amount.toString(),
    timestamp: Date.now().toString(),
  };

  // XADD is the Redis command that appends to a stream.
  // "*" means "auto-generate the entry ID" (Redis handles ordering for us).
  const id = await redis.xadd(STREAM_KEY, "*", event);
  console.log(`[emitted] id=${id} user=${userId} amount=$${amount}`);
}

// Occasionally simulate a "burst" — one user making many rapid purchases.
// This is the pattern our anomaly detector will later learn to catch.
async function emitBurst() {
  const burstUser = randomUser();
  const burstSize = 8 + Math.floor(Math.random() * 8); // 8-15 events
  console.log(`\n[BURST] simulating ${burstSize} rapid events for ${burstUser}\n`);

  for (let i = 0; i < burstSize; i++) {
    await emitEvent(burstUser, randomAmount());
    await new Promise((r) => setTimeout(r, 150)); // rapid-fire, 150ms apart
  }
}

// Main loop: emit a normal event every ~1 second,
// with roughly a 5% chance each tick to trigger a burst instead.
async function main() {
  console.log(`Producer started. Writing to stream: ${STREAM_KEY}`);

  setInterval(async () => {
    try {
      const isBurst = Math.random() < 0.05;
      if (isBurst) {
        await emitBurst();
      } else {
        await emitEvent(randomUser(), randomAmount());
      }
    } catch (err) {
      console.error("Failed to emit event:", err);
    }
  }, 1000);
}

main();