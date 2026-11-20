import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const SCHEMA = `
CREATE TABLE IF NOT EXISTS pipelines (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  definition TEXT NOT NULL,
  graph_hash TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  pipeline_id TEXT NOT NULL REFERENCES pipelines(id),
  pipeline_name TEXT NOT NULL,
  graph_hash TEXT NOT NULL,
  definition TEXT NOT NULL,
  status TEXT NOT NULL,
  gate TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  started_at INTEGER,
  finished_at INTEGER,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  snapshot TEXT
);

CREATE TABLE IF NOT EXISTS node_runs (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  node_id TEXT NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  outputs TEXT,
  inputs TEXT,
  meta TEXT,
  error TEXT,
  skip_reason TEXT,
  logs TEXT NOT NULL DEFAULT '[]',
  started_at INTEGER,
  finished_at INTEGER,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (run_id, node_id)
);

CREATE TABLE IF NOT EXISTS events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  node_id TEXT,
  type TEXT NOT NULL,
  message TEXT NOT NULL,
  detail TEXT,
  at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS side_effects (
  run_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  effect_key TEXT NOT NULL,
  payload TEXT,
  committed_at INTEGER NOT NULL,
  PRIMARY KEY (run_id, node_id, effect_key)
);

CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id, seq);
CREATE INDEX IF NOT EXISTS idx_node_runs_run ON node_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
`;

export function openStore(filePath) {
  fs.mkdirSync(path.dirname(path.resolve(filePath)), { recursive: true });
  const db = new DatabaseSync(filePath);
  db.exec('PRAGMA journal_mode = WAL');
  db.exec('PRAGMA synchronous = NORMAL');
  db.exec('PRAGMA foreign_keys = ON');
  db.exec(SCHEMA);
  return new Store(db);
}

export class Store {
  constructor(db) {
    this.db = db;
  }

  close() {
    this.db.close();
  }

  // ---- pipelines -----------------------------------------------------------
  upsertPipeline(graph) {
    const now = Date.now();
    this.db
      .prepare(
        `INSERT INTO pipelines (id, name, definition, graph_hash, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET name=excluded.name, definition=excluded.definition,
           graph_hash=excluded.graph_hash, updated_at=excluded.updated_at`,
      )
      .run(graph.id, graph.name, JSON.stringify(graph), graph.hash, now, now);
    return this.getPipeline(graph.id);
  }

  getPipeline(id) {
    return this.db.prepare('SELECT * FROM pipelines WHERE id = ?').get(id) ?? null;
  }

  listPipelines() {
    return this.db.prepare('SELECT * FROM pipelines ORDER BY updated_at DESC').all();
  }

  // ---- runs ---------------------------------------------------------------
  createRun({ id, graph, status = 'running', gate = null }) {
    const now = Date.now();
    this.db
      .prepare(
        `INSERT INTO runs (id, pipeline_id, pipeline_name, graph_hash, definition, status, gate,
           created_at, updated_at, started_at, cancel_requested, snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)`,
      )
      .run(id, graph.id, graph.name, graph.hash, JSON.stringify(graph), status, gate, now, now, now);
    const insertNode = this.db.prepare(
      `INSERT INTO node_runs (run_id, node_id, status, attempts, logs, updated_at)
       VALUES (?, ?, 'pending', 0, '[]', ?)`,
    );
    for (const node of graph.nodes) insertNode.run(id, node.id, now);
    return this.getRun(id);
  }

  getRun(id) {
    return this.db.prepare('SELECT * FROM runs WHERE id = ?').get(id) ?? null;
  }

  listRuns(limit = 50) {
    return this.db
      .prepare('SELECT id, pipeline_id, pipeline_name, status, gate, created_at, updated_at, started_at, finished_at FROM runs ORDER BY created_at DESC LIMIT ?')
      .all(limit);
  }

  listLiveRuns() {
    return this.db.prepare("SELECT * FROM runs WHERE status IN ('running','paused')").all();
  }

  updateRun(id, fields) {
    const allowed = ['status', 'gate', 'updated_at', 'finished_at', 'cancel_requested', 'snapshot'];
    const sets = [];
    const values = [];
    for (const [key, value] of Object.entries(fields)) {
      if (!allowed.includes(key)) throw new Error(`不可更新的 run 字段：${key}`);
      sets.push(`${key} = ?`);
      values.push(value);
    }
    if (!sets.length) return;
    values.push(id);
    this.db.prepare(`UPDATE runs SET ${sets.join(', ')} WHERE id = ?`).run(...values);
  }

  // ---- node runs ----------------------------------------------------------
  getNodeRun(runId, nodeId) {
    return this.db.prepare('SELECT * FROM node_runs WHERE run_id = ? AND node_id = ?').get(runId, nodeId) ?? null;
  }

  listNodeRuns(runId) {
    return this.db.prepare('SELECT * FROM node_runs WHERE run_id = ?').all(runId);
  }

  updateNodeRun(runId, nodeId, fields) {
    const allowed = ['status', 'attempts', 'outputs', 'inputs', 'meta', 'error', 'skip_reason', 'logs', 'started_at', 'finished_at', 'updated_at'];
    const sets = [];
    const values = [];
    for (const [key, value] of Object.entries(fields)) {
      if (!allowed.includes(key)) throw new Error(`不可更新的 node_run 字段：${key}`);
      sets.push(`${key} = ?`);
      values.push(value);
    }
    if (!sets.length) return;
    values.push(runId, nodeId);
    this.db.prepare(`UPDATE node_runs SET ${sets.join(', ')} WHERE run_id = ? AND node_id = ?`).run(...values);
  }

  appendNodeLog(runId, nodeId, entry) {
    const row = this.getNodeRun(runId, nodeId);
    const logs = row?.logs ? JSON.parse(row.logs) : [];
    logs.push(entry);
    // keep the summary bounded; full history lives in the events table
    const trimmed = logs.length > 200 ? logs.slice(logs.length - 200) : logs;
    this.updateNodeRun(runId, nodeId, { logs: JSON.stringify(trimmed), updated_at: Date.now() });
  }

  // ---- events -------------------------------------------------------------
  appendEvent(runId, nodeId, type, message, detail = null) {
    const at = Date.now();
    const info = this.db
      .prepare('INSERT INTO events (run_id, node_id, type, message, detail, at) VALUES (?, ?, ?, ?, ?, ?)')
      .run(runId, nodeId, type, message, detail == null ? null : JSON.stringify(detail), at);
    return Number(info.lastInsertRowid);
  }

  listEvents(runId, afterSeq = 0, limit = 500) {
    return this.db
      .prepare('SELECT * FROM events WHERE run_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?')
      .all(runId, afterSeq, limit)
      .map((e) => ({ ...e, detail: e.detail ? JSON.parse(e.detail) : null }));
  }

  // ---- side effects -------------------------------------------------------
  hasEffect(runId, nodeId, effectKey) {
    return Boolean(
      this.db
        .prepare('SELECT 1 AS hit FROM side_effects WHERE run_id = ? AND node_id = ? AND effect_key = ?')
        .get(runId, nodeId, effectKey),
    );
  }

  commitEffect(runId, nodeId, effectKey, payload) {
    this.db
      .prepare(
        `INSERT INTO side_effects (run_id, node_id, effect_key, payload, committed_at) VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(run_id, node_id, effect_key) DO NOTHING`,
      )
      .run(runId, nodeId, effectKey, payload == null ? null : JSON.stringify(payload), Date.now());
  }

  listEffects(runId) {
    return this.db
      .prepare('SELECT * FROM side_effects WHERE run_id = ? ORDER BY committed_at ASC')
      .all(runId)
      .map((e) => ({ ...e, payload: e.payload ? JSON.parse(e.payload) : null }));
  }
}
