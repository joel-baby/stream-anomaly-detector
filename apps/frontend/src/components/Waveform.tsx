import { useEffect, useRef, useState } from "react";
import { socket } from "../socket";
import type { FlaggedEvent } from "../types";

type Point = { x: number; y: number };

const WIDTH = 800;
const HEIGHT = 120;
const BASELINE_Y = HEIGHT / 2;
const MAX_POINTS = 120;
const TICK_MS = 100; // how often the line advances

export function Waveform() {
  const [points, setPoints] = useState<Point[]>(
    Array.from({ length: MAX_POINTS }, (_, i) => ({ x: i, y: BASELINE_Y }))
  );
  const pendingSpike = useRef(0); // how many "ticks" of spike remain
  const spikeHeight = useRef(0);

  useEffect(() => {
    function handleAnomaly(event: FlaggedEvent) {
      const ratio = event.windowCount / (event.baselineAvgCount || 1);
      // Cap how tall the spike can visually get, so one extreme outlier
      // doesn't flatten every other spike into invisibility.
      const height = Math.min(ratio * 8, HEIGHT / 2 - 10);
      spikeHeight.current = height;
      pendingSpike.current = 6; // spike lasts ~6 ticks, then decays
    }

    socket.on("anomaly", handleAnomaly);
    return () => {
      socket.off("anomaly", handleAnomaly);
    };
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      setPoints((prev) => {
        let y = BASELINE_Y;

        if (pendingSpike.current > 0) {
          // Simple decay curve so the spike rises then falls, not a
          // hard square wave — reads more like an instrument, less
          // like a bar chart.
          const progress = pendingSpike.current / 6;
          y = BASELINE_Y - spikeHeight.current * Math.sin(progress * Math.PI);
          pendingSpike.current -= 1;
        } else {
          // Gentle idle jitter so the line doesn't look frozen/dead
          // when nothing is happening.
          y = BASELINE_Y + (Math.random() - 0.5) * 3;
        }

        const next = [...prev.slice(1), { x: 0, y }];
        return next.map((p, i) => ({ ...p, x: i }));
      });
    }, TICK_MS);

    return () => clearInterval(interval);
  }, []);

  const isSpiking = pendingSpike.current > 0;
  const pathD = points
    .map((p, i) => `${i === 0 ? "M" : "L"} ${(p.x / (MAX_POINTS - 1)) * WIDTH} ${p.y}`)
    .join(" ");

  return (
    <div
      style={{
        border: "1px solid var(--color-grid)",
        background: "var(--color-panel)",
        padding: "1rem",
      }}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          marginBottom: "0.5rem",
        }}
      >
        <span className="mono" style={{ fontSize: "0.75rem", color: "var(--color-ink-muted)" }}>
          SIGNAL DEVIATION
        </span>
        <span
          className="mono"
          style={{
            fontSize: "0.75rem",
            color: isSpiking ? "var(--color-signal-alert)" : "var(--color-signal-safe)",
          }}
        >
          {isSpiking ? "● ANOMALY" : "● NOMINAL"}
        </span>
      </div>

      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} width="100%" height={HEIGHT}>
        <line
          x1="0"
          y1={BASELINE_Y}
          x2={WIDTH}
          y2={BASELINE_Y}
          stroke="var(--color-grid)"
          strokeDasharray="4 4"
        />
        <path
          d={pathD}
          fill="none"
          stroke={isSpiking ? "var(--color-signal-alert)" : "var(--color-signal-safe)"}
          strokeWidth="2"
        />
      </svg>
    </div>
  );
}