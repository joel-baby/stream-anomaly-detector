import { useEffect, useRef, useState } from "react";
import { socket } from "../socket";
import type { FlaggedEvent } from "../types";

type Point = { x: number; y: number };

const WIDTH = 800;
const HEIGHT = 120;
const BASELINE_Y = HEIGHT / 2;
const MAX_POINTS = 120;
const TICK_MS = 100;

export function Waveform() {
  const [points, setPoints] = useState<Point[]>(
    Array.from({ length: MAX_POINTS }, (_, i) => ({ x: i, y: BASELINE_Y }))
  );
  const [isSpiking, setIsSpiking] = useState(false);
  const pendingSpike = useRef(0);
  const spikeHeight = useRef(0);

  useEffect(() => {
    function handleAnomaly(event: FlaggedEvent) {
      const ratio = event.windowCount / (event.baselineAvgCount || 1);
      const height = Math.min(ratio * 8, HEIGHT / 2 - 10);
      spikeHeight.current = height;
      pendingSpike.current = 6;
    }

    socket.on("anomaly", handleAnomaly);
    return () => {
      socket.off("anomaly", handleAnomaly);
    };
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      setPoints((prev) => {
        let y: number;

        if (pendingSpike.current > 0) {
          const progress = pendingSpike.current / 6;
          y = BASELINE_Y - spikeHeight.current * Math.sin(progress * Math.PI);
          pendingSpike.current -= 1;
        } else {
          y = BASELINE_Y + (Math.random() - 0.5) * 3;
        }

        const next = [...prev.slice(1), { x: 0, y }];
        return next.map((p, i) => ({ ...p, x: i }));
      });

      // Update spiking state here, inside the interval callback — not
      // read directly from the ref during render, which React's rules
      // now flag as unsafe (ref reads should happen in effects/handlers,
      // never in the render body itself).
      setIsSpiking(pendingSpike.current > 0);
    }, TICK_MS);

    return () => clearInterval(interval);
  }, []);

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