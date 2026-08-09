import { useEffect, useState } from "react";
import { socket } from "../socket";
import type { FlaggedEvent } from "../types";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:4000";

export function LiveTicker() {
  const [events, setEvents] = useState<FlaggedEvent[]>([]);
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    // Load recent history first, so the dashboard isn't empty on open.
    fetch(`${API_URL}/api/flagged-events?limit=20`)
      .then((res) => res.json())
      .then((data: FlaggedEvent[]) => setEvents(data));

    // Then listen for new live anomalies and prepend them as they arrive.
    function handleAnomaly(event: FlaggedEvent) {
      setEvents((prev) => [event, ...prev].slice(0, 50)); // cap the list
    }

    socket.on("connect", () => setConnected(true));
    socket.on("disconnect", () => setConnected(false));
    socket.on("anomaly", handleAnomaly);

    return () => {
      socket.off("connect");
      socket.off("disconnect");
      socket.off("anomaly", handleAnomaly);
    };
  }, []);

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", marginBottom: "1rem" }}>
        <span
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            background: connected ? "var(--color-signal-safe)" : "var(--color-signal-alert)",
            display: "inline-block",
          }}
        />
        <span className="mono" style={{ fontSize: "0.8rem", color: "var(--color-ink-muted)" }}>
          {connected ? "LIVE" : "DISCONNECTED"}
        </span>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
        {events.length === 0 && (
          <p style={{ color: "var(--color-ink-muted)" }}>No anomalies flagged yet.</p>
        )}

        {events.map((event) => (
          <div
            key={event._id}
            style={{
              border: "1px solid var(--color-grid)",
              borderLeft: "3px solid var(--color-signal-alert)",
              background: "var(--color-panel)",
              padding: "0.75rem 1rem",
              display: "flex",
              justifyContent: "space-between",
              alignItems: "baseline",
            }}
          >
            <div>
              <span className="mono" style={{ fontWeight: 600 }}>{event.userId}</span>
              <span style={{ color: "var(--color-ink-muted)", marginLeft: "0.75rem", fontSize: "0.85rem" }}>
                {event.windowCount} events · ${event.windowSpend.toFixed(2)} in 5m
              </span>
            </div>
            <span className="mono" style={{ color: "var(--color-signal-alert)", fontSize: "0.85rem" }}>
              {(event.windowCount / event.baselineAvgCount).toFixed(1)}x baseline
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}