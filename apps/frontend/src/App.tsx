import "./App.css";
import { LiveTicker } from "./components/LiveTicker";
import { Waveform } from "./components/Waveform";
import { Sidebar } from "./components/Sidebar";

function App() {
  return (
    <div className="console">
      <aside
        style={{
          borderRight: "1px solid var(--color-grid)",
          padding: "1.5rem",
        }}
      >
        <Sidebar />
      </aside>
      <main style={{ padding: "1.5rem" }}>
        <Waveform />
        <h1 style={{ margin: "1.5rem 0" }}>Live Feed</h1>
        <LiveTicker />
      </main>
    </div>
  );
}

export default App;
