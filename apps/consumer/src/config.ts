import "dotenv/config";

export const PREFIX = process.env.REDIS_KEY_PREFIX ?? "stream-anomaly";
export const STREAM_KEY = `${PREFIX}:transactions`;
export const GROUP_NAME = `${PREFIX}:consumer-group`;
export const CONSUMER_NAME = "consumer-1";

export const WINDOW_MS = 5 * 60 * 1000; // 5 minutes

export const ANOMALY_MULTIPLIER = parseFloat(process.env.ANOMALY_MULTIPLIER ?? "4");
export const EWMA_ALPHA = parseFloat(process.env.EWMA_ALPHA ?? "0.2");
export const MIN_SAMPLES_BEFORE_SCORING = 3;
export const BASELINE_UPDATE_INTERVAL_MS = parseInt(
  process.env.BASELINE_UPDATE_INTERVAL_MS ?? "60000",
  10
);

export const MONGODB_URI = process.env.MONGODB_URI!;
export const MONGODB_DB_NAME = process.env.MONGODB_DB_NAME ?? "stream-anomaly";