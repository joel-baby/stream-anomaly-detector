import { MongoClient, Collection } from "mongodb";
import { MONGODB_URI, MONGODB_DB_NAME } from "./config.js";

const mongoClient = new MongoClient(MONGODB_URI);
let flaggedEventsCollection: Collection;

export async function connectMongo() {
  await mongoClient.connect();
  const db = mongoClient.db(MONGODB_DB_NAME);
  flaggedEventsCollection = db.collection("flagged_events");
  console.log("Connected to MongoDB");
}

export type FlaggedEvent = {
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

export async function persistFlaggedEvent(event: FlaggedEvent) {
  try {
    await flaggedEventsCollection.insertOne(event);
  } catch (err) {
    console.error("Failed to persist flagged event to MongoDB:", err);
  }
}