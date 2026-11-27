import { useState } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

type Row = Record<string, unknown>;
type Layer = "bronze" | "silver" | "gold";
type Run = {
  id: string; status: string; input?: number; valid?: number; invalid?: number; message?: string;
  schema: string[]; datasets: Record<Layer, Row[]>; quarantine?: {original: Row; reason: string; fields: string[]}[];
  lineage?: { stage: Layer; parent: Layer | null; records: number; restored: boolean }[];
  retries?: Row[]; restored_stages?: string[]; compatible_additions?: string[];
};
type History = {id: string; created: number; status: string};
const api = (import.meta.env.VITE_API_URL || "http://localhost:8000").replace(/\/$/, "");
const scenarios = {
  none: "Healthy pipeline", duplicate: "Duplicate order", invalid: "Invalid amount",
  added_column: "Compatible column addition", removed_field: "Breaking schema change",
  temporary_source: "Transient timeout", gold: "Gold stage failure",
};

function Table({ rows }: {rows: Row[]}) {
  const columns = Array.from(new Set(rows.flatMap(Object.keys)));
  if (!rows.length) return <p className="muted">No records for this view.</p>;
  return <div className="table-wrap"><table><thead><tr>{columns.map(column => <th key={column}>{column}</th>)}</tr></thead>
    <tbody>{rows.slice(0, 30).map((row, index) => <tr key={index}>{columns.map(column =>
      <td key={column}>{row[column] == null ? "—" : typeof row[column] === "object" ? JSON.stringify(row[column]) : String(row[column])}</td>)}</tr>)}</tbody>
  </table>{rows.length > 30 && <p className="muted">Showing 30 of {rows.length} records.</p>}</div>;
}

function App() {
  const [token, setToken] = useState("");
  const [current, setCurrent] = useState<Run>();
  const [history, setHistory] = useState<History[]>([]);
  const [layer, setLayer] = useState<Layer>("bronze");
  const [scenario, setScenario] = useState<keyof typeof scenarios>("none");
  const [records, setRecords] = useState(100);
  const [query, setQuery] = useState<{sql: string; rows: Row[]}>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [light, setLight] = useState(false);

  async function request(path: string, method = "GET", body?: unknown) {
    const headers: Record<string, string> = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    if (body !== undefined) headers["Content-Type"] = "application/json";
    const response = await fetch(api + path, {method, headers, body: body === undefined ? undefined : JSON.stringify(body)});
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(typeof payload.detail === "string" ? payload.detail : `Request failed (${response.status})`);
    }
    return response;
  }

  async function perform(action: () => Promise<void>) {
    setBusy(true);
    setError("");
    try { await action(); }
    catch (failure) { setError(failure instanceof Error ? failure.message : "Unexpected failure."); }
    finally { setBusy(false); }
  }

  async function startDemo() {
    const response = await request("/demo/sessions", "POST");
    setToken((await response.json()).token);
  }

  async function startRun() {
    const response = await request("/demo/runs", "POST", {records, failure: scenario});
    setCurrent(await response.json());
    setQuery(undefined);
    setHistory(await (await request("/demo/runs")).json());
  }

  async function exportGold(runId: string) {
    const response = await request(`/demo/runs/${runId}/gold.csv`);
    const url = URL.createObjectURL(await response.blob());
    const link = document.createElement("a");
    link.href = url;
    link.download = "pipelinex-gold.csv";
    link.click();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  return <div className={light ? "shell light" : "shell"}>
    <aside><strong>Pipeline<span>X</span></strong><nav aria-label="Workspace">
      {[["overview", "Overview"], ["lab", "Failure Lab"], ["lineage", "Data lineage"], ["quality", "Quality & recovery"], ["sql", "SQL workspace"], ["history", "Run history"]].map(([id, label]) =>
        <a key={id} href={`#${id}`}>{label}</a>)}</nav>
    </aside>
    <main>
      <header><div><h1>Pipeline control room</h1><p className="muted">Synthetic retail data · Bounded Python demo engine</p></div>
        <button onClick={() => setLight(value => !value)} aria-pressed={light}>{light ? "Dark theme" : "Light theme"}</button></header>
      {error && <p role="alert" className="error">{error}</p>}
      {busy && <p role="status">Working…</p>}
      {!token && <section id="overview"><h2>Test how a pipeline recovers</h2><p>Run a synthetic batch, inject a failure, and inspect the records and recovery decisions.</p>
        <button className="primary" disabled={busy} onClick={() => void perform(startDemo)}>Try Live Demo</button></section>}
      {token && <section id="lab"><h2>Failure Simulation Lab</h2><form onSubmit={event => {event.preventDefault(); void perform(startRun);}}>
        <label>Input orders<input type="number" min="1" max="500" required value={records} onChange={event => setRecords(event.target.valueAsNumber)} /></label>
        <label>Scenario<select value={scenario} onChange={event => setScenario(event.target.value as keyof typeof scenarios)}>
          {Object.entries(scenarios).map(([value, text]) => <option key={value} value={value}>{text}</option>)}</select></label>
        <button className="primary" disabled={busy}>Execute pipeline</button></form></section>}
      {current && <>
        <section id="overview"><h2>Execution overview</h2><p className="status">{current.status.replaceAll("_", " ")}</p>
          <div className="metrics">{[["Bronze records", current.datasets.bronze.length], ["Valid records", current.valid], ["Quarantined", current.invalid], ["Gold groups", current.datasets.gold.length]].map(([name, value]) =>
            <div key={name}><span>{name}</span><strong>{value ?? "—"}</strong></div>)}</div></section>
        <section id="lineage"><h2>Lineage & dataset explorer</h2><div className="stages">
          {(current.lineage ?? []).map(({stage, parent, records, restored}) => <div key={stage}>
            {parent && <span aria-hidden="true">→ </span>}
            <button aria-pressed={layer === stage} onClick={() => setLayer(stage)}>{stage} · {records}{restored ? " · restored" : ""}</button>
          </div>)}</div>
          <Table rows={current.datasets[layer]} /><h3>Observed Bronze schema</h3><p>{current.schema.join(" · ")}</p>
          {!!current.compatible_additions?.length && <p>Compatible additions: {current.compatible_additions.join(", ")}</p>}</section>
        <section id="quality"><h2>Quality & recovery</h2>{current.message && <p className="error">{current.message}</p>}
          <p>Restored checkpoints: {current.restored_stages?.join(", ") || "none"}</p>
          <p>Source retries: {current.retries?.length ?? 0}</p><Table rows={current.retries ?? []} />
          <h3>Quarantine</h3><Table rows={current.quarantine ?? []} /></section>
        <section id="sql"><h2>SQL workspace</h2><p className="muted">Read-only queries over this run’s Gold dataset.</p>
          <div className="stages">{(["products", "totals"] as const).map(view => <button key={view} disabled={busy || !current.datasets.gold.length}
            onClick={() => void perform(async () => setQuery(await (await request(`/demo/runs/${current.id}/query?view=${view}`)).json()))}>{view}</button>)}
            <button disabled={busy || !current.datasets.gold.length} onClick={() => void perform(() => exportGold(current.id))}>Download Gold CSV</button></div>
          {query && <><pre>{query.sql}</pre><Table rows={query.rows} /></>}</section>
        <section id="history"><h2>Run history</h2><ul>{history.map(item => <li key={item.id}>
          <button disabled={busy} onClick={() => void perform(async () => {setCurrent(await (await request(`/demo/runs/${item.id}`)).json()); setQuery(undefined);})}>
            {new Date(item.created * 1000).toLocaleString()} · {item.status.replaceAll("_", " ")} · {item.id.slice(0, 8)}</button>
        </li>)}</ul></section>
      </>}
    </main>
  </div>;
}

createRoot(document.getElementById("root")!).render(<App />);
