export type FlaggedEvent = {
  _id: string;
  userId: string;
  eventId: string;
  amount: number;
  timestamp: number;
  windowCount: number;
  windowSpend: number;
  baselineAvgCount: number;
  baselineAvgSpend: number;
  flaggedAt: string;
};