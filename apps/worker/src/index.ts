import { Queue, Worker } from "bullmq";
import { Redis as IORedis } from "ioredis";
import { MongoClient } from "mongodb";
import "dotenv/config";

const connection = new IORedis(process.env.UPSTASH_REDIS_TCP_URL!, {
  maxRetriesPerRequest: null, // required by BullMQ
});

const QUEUE_NAME = "replay-queue";
const replayQueue = new Queue(QUEUE_NAME, { connection });

type ReplayJobData = {
  startTime: number; // epoch ms
  endTime: number;
};

// The actual replay logic: pull raw events in the range, run each
// through a simplified version of the same windowing/scoring logic,
// using the REPLAY_KEY_PREFIX namespace so it never touches live state.
async function processReplayJob(data: ReplayJobData) {
  const mongoClient = new MongoClient(process.env.MONGODB_URI!);
  await mongoClient.connect();
  const db = mongoClient.db(process.env.MONGODB_DB_NAME ?? "stream-anomaly");
  const rawEvents = db.collection("raw_events");

  const cursor = rawEvents
    .find({
      timestamp: {
        $gte: new Date(data.startTime),
        $lte: new Date(data.endTime),
      },
    })
    .sort({ timestamp: 1 });

  let processed = 0;
  let flagged = 0;

  const REPLAY_PREFIX =
    process.env.REPLAY_KEY_PREFIX ?? "stream-anomaly-replay";
  const redisRest = connection; // reuse the same ioredis connection for simplicity here

  const keysToClear = await redisRest.keys(`${REPLAY_PREFIX}:*`);
  if (keysToClear.length > 0) {
    await redisRest.del(...keysToClear);
  }

  for await (const doc of cursor) {
    const userId = doc.userId as string;
    const amount = doc.amount as number;
    const eventId = doc.eventId as string;
    const ts = (doc.timestamp as Date).getTime();

    const windowKey = `${REPLAY_PREFIX}:window:${userId}`;
    const baselineKey = `${REPLAY_PREFIX}:baseline:${userId}`;

    const member = `${amount}:${eventId}`; // eventId, not a random string
    await redisRest.zadd(windowKey, ts, member);
    const cutoff = ts - 5 * 60 * 1000;
    await redisRest.zremrangebyscore(windowKey, 0, cutoff);

    const members = await redisRest.zrangebyscore(windowKey, cutoff, "+inf");
    let totalSpend = 0;
    for (const m of members) {
      totalSpend += parseFloat(m.split(":")[0]);
    }
    const count = members.length;

    const rawBaseline = await redisRest.get(baselineKey);
    const baseline = rawBaseline
      ? JSON.parse(rawBaseline)
      : { avgCount: 0, avgSpend: 0, samples: 0 };

    const multiplier = parseFloat(process.env.ANOMALY_MULTIPLIER ?? "4");
    const isAnomaly =
      baseline.samples >= 3 &&
      baseline.avgCount > 0 &&
      (count / baseline.avgCount >= multiplier ||
        (baseline.avgSpend > 0 &&
          totalSpend / baseline.avgSpend >= multiplier));

    if (isAnomaly) {
      flagged++;
      console.log(
        `🚨 flagged user=${userId} count=${count} baselineAvgCount=${baseline.avgCount.toFixed(2)}`,
      );
    } else {
      const lastUpdateKey = `${REPLAY_PREFIX}:baseline-updated:${userId}`;
      const lastUpdate = await redisRest.get(lastUpdateKey);
      const shouldUpdate =
        !lastUpdate ||
        ts - parseInt(lastUpdate, 10) >= BASELINE_UPDATE_INTERVAL_MS;

      if (shouldUpdate) {
        const alpha = parseFloat(process.env.EWMA_ALPHA ?? "0.2");
        const updated = {
          avgCount:
            baseline.samples === 0
              ? count
              : alpha * count + (1 - alpha) * baseline.avgCount,
          avgSpend:
            baseline.samples === 0
              ? totalSpend
              : alpha * totalSpend + (1 - alpha) * baseline.avgSpend,
          samples: baseline.samples + 1,
        };
        await redisRest.set(baselineKey, JSON.stringify(updated));
        await redisRest.set(lastUpdateKey, ts.toString());
      }
    }

    processed++;
  }

  await mongoClient.close();
  console.log(`Replay complete: processed=${processed} flagged=${flagged}`);
  return { processed, flagged };
}

const BASELINE_UPDATE_INTERVAL_MS = 60000; // same interval as the live consumer

// The worker listens for jobs on the queue and runs processReplayJob.
const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    console.log(`Starting replay job ${job.id}, range:`, job.data);
    return processReplayJob(job.data as ReplayJobData);
  },
  { connection },
);

worker.on("completed", (job, result) => {
  console.log(`Job ${job.id} completed:`, result);
});

worker.on("failed", (job, err) => {
  console.error(`Job ${job?.id} failed:`, err.message);
});

console.log("Replay worker started, waiting for jobs...");

// For manual testing: if run with CLI args, enqueue a job immediately
// instead of just idling and waiting.
const args = process.argv.slice(2);
if (args[0] === "run") {
  const hoursBack = parseFloat(args[1] ?? "1");
  const endTime = Date.now();
  const startTime = endTime - hoursBack * 60 * 60 * 1000;

  replayQueue.add("replay", { startTime, endTime }).then((job) => {
    console.log(`Enqueued replay job ${job.id} for last ${hoursBack} hour(s)`);
  });
}
