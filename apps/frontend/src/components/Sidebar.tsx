import { useEffect, useState } from "react";
import { socket } from "../socket";
import type { FlaggedEvent } from "../types";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:4000";

type UserStat = {
  _id: string;
  flagCount: number;
  totalFlaggedSpend: number;
  lastFlaggedAt: string;
};

export function Sidebar() {
  const [stats, setStats] = useState<UserStat[]>([]);

  function loadStats() {
    fetch(`${API_URL}/api/stats`)
      .then((res) => res.json())
      .then(setStats);
  }

  useEffect(() => {
    loadStats();

    // Refresh the aggregated stats whenever a new anomaly comes in,
    // rather than trying to update counts by hand on the client —
    // simpler and always correct, at the cost of one extra fetch per event.
    function handleAnomaly() {
      loadStats();
    }

    socket.on("anomaly", handleAnomaly);
    return () => {
      socket.off("anomaly", handleAnomaly);
    };
  }, []);

  return (
    <div>
      <h2 style={{ marginBottom: "1rem" }}>Instrument Index</h2>
      <p
        className="mono"
        style={{
          fontSize: "0.7rem",
          color: "var(--color-ink-muted)",
          marginBottom: "1rem",
        }}
      >
        MOST FLAGGED USERS
      </p>

      <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
        {stats.length === 0 && (
          <p style={{ color: "var(--color-ink-muted)", fontSize: "0.85rem" }}>
            No data yet.
          </p>
        )}

        {stats.map((s, i) => (
          <div
            key={s._id}
            style={{
              display: "flex",
              justifyContent: "space-between",
              borderBottom: "1px solid var(--color-grid)",
              paddingBottom: "0.5rem",
            }}
          >
            <div>
              <span
                className="mono"
                style={{
                  color: "var(--color-ink-muted)",
                  marginRight: "0.5rem",
                }}
              >
                {String(i + 1).padStart(2, "0")}
              </span>
              <span className="mono">{s._id}</span>
            </div>
            <span
              className="mono"
              style={{
                color: "var(--color-signal-alert)",
                fontSize: "0.85rem",
              }}
            >
              {s.flagCount}×
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
