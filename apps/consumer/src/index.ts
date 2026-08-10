import {
  ensureGroupExists,
  fieldsToObject,
  readEvents,
  acknowledge,
} from "./streamConsumer.js";
import { recordInWindow, getWindowStats } from "./windowing.js";
import {
  getBaseline,
  updateBaseline,
  isAnomalous,
  shouldUpdateBaseline,
  markBaselineUpdated,
} from "./baseline.js";
import {
  connectMongo,
  persistFlaggedEvent,
  archiveRawEvent,
} from "./mongoClient.js";
import { CONSUMER_NAME, STREAM_KEY } from "./config.js";

async function main() {
  await connectMongo();
  await ensureGroupExists();
  console.log(
    `Consumer "${CONSUMER_NAME}" started. Listening on: ${STREAM_KEY}\n`,
  );

  while (true) {
    try {
      const result = await readEvents();

      if (!result) {
        await new Promise((r) => setTimeout(r, 500));
        continue;
      }

      for (const [, messages] of result) {
        for (const [id, fields] of messages) {
          const { userId, amount, timestamp } = fieldsToObject(fields);
          const ts = parseInt(timestamp, 10);
          const amt = parseFloat(amount);
          await archiveRawEvent({
            userId,
            eventId: id,
            amount: amt,
            timestamp: new Date(ts),
          });

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

          if (!flagged && (await shouldUpdateBaseline(userId, ts))) {
            await updateBaseline(userId, stats.count, stats.totalSpend);
            await markBaselineUpdated(userId, ts);
          }

          await acknowledge(id);
        }
      }
    } catch (err) {
      console.error("Error reading from stream:", err);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

main();
