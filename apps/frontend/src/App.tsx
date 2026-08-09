import "./App.css";
import { LiveTicker } from "./components/LiveTicker";

function App() {
  return (
    <div className="console">
      <aside style={{ borderRight: "1px solid var(--color-grid)", padding: "1.5rem" }}>
        <h2>Instrument Index</h2>
        <p style={{ color: "var(--color-ink-muted)" }}>Sidebar goes here</p>
      </aside>
      <main style={{ padding: "1.5rem" }}>
        <h1 style={{ marginBottom: "1.5rem" }}>Live Feed</h1>
        <LiveTicker />
      </main>
    </div>
  );
}

export default App;