import express from "express";
import cors from "cors";
import { createServer } from "http";
import { Server as SocketIOServer } from "socket.io";
import { MongoClient, ChangeStream } from "mongodb";
import "dotenv/config";

const PORT = process.env.PORT ?? 4000;
const MONGODB_URI = process.env.MONGODB_URI!;
const MONGODB_DB_NAME = process.env.MONGODB_DB_NAME ?? "stream-anomaly";
const CORS_ORIGIN = process.env.CORS_ORIGIN ?? "http://localhost:5173";

const app = express();
app.use(cors({ origin: CORS_ORIGIN }));

const httpServer = createServer(app);

// Socket.io attaches to the same underlying HTTP server as Express,
// so both the REST endpoint and the WebSocket run on one port.
const io = new SocketIOServer(httpServer, {
  cors: { origin: CORS_ORIGIN },
});

const mongoClient = new MongoClient(MONGODB_URI);

async function main() {
  await mongoClient.connect();
  console.log("Connected to MongoDB");

  const db = mongoClient.db(MONGODB_DB_NAME);
  const flaggedEvents = db.collection("flagged_events");

  // REST endpoint: lets the dashboard load recent history on first open,
  // before any new live anomalies have occurred.
  app.get("/api/flagged-events", async (req, res) => {
    const limit = parseInt((req.query.limit as string) ?? "50", 10);
    const events = await flaggedEvents
      .find({})
      .sort({ flaggedAt: -1 })
      .limit(limit)
      .toArray();
    res.json(events);
  });

  app.get("/api/health", (req, res) => {
    res.json({ status: "ok" });
  });

  app.get("/api/stats", async (req, res) => {
  const stats = await flaggedEvents
    .aggregate([
      {
        $group: {
          _id: "$userId",
          flagCount: { $sum: 1 },
          totalFlaggedSpend: { $sum: "$windowSpend" },
          lastFlaggedAt: { $max: "$flaggedAt" },
        },
      },
      { $sort: { flagCount: -1 } },
      { $limit: 10 },
    ])
    .toArray();

  res.json(stats);
});

  // Change Streams: MongoDB pushes us a notification the instant a new
  // document is inserted into flagged_events, instead of us having to
  // repeatedly poll the collection. This only works because MongoDB
  // Atlas free tier runs as a replica set (a requirement for Change
  // Streams) — a plain standalone MongoDB would NOT support this.
  const changeStream: ChangeStream = flaggedEvents.watch([
    { $match: { operationType: "insert" } },
  ]);

  changeStream.on("change", (change: any) => {
    const newEvent = change.fullDocument;
    console.log(`[live] pushing new anomaly for user=${newEvent.userId}`);
    io.emit("anomaly", newEvent);
  });

  changeStream.on("error", (err) => {
    console.error("Change stream error:", err);
  });

  io.on("connection", (socket) => {
    console.log(`Dashboard connected: ${socket.id}`);
    socket.on("disconnect", () => {
      console.log(`Dashboard disconnected: ${socket.id}`);
    });
  });

  httpServer.listen(PORT, () => {
    console.log(`API + WebSocket server running on http://localhost:${PORT}`);
  });
}

main().catch((err) => {
  console.error("Failed to start API server:", err);
  process.exit(1);
});