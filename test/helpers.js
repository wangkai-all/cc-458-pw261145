import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Engine } from '../src/engine.js';
import { createExecutors } from '../src/executors.js';
import { openStore } from '../src/store.js';
import { readJson } from '../src/util.js';

export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const FIXTURES = path.join(ROOT, 'fixtures');

export function loadFixture(name) {
  return readJson(path.join(FIXTURES, 'graphs', name));
}

/**
 * Boots a full engine over a throwaway data dir. `outbox` counts the publish
 * side effects so idempotency assertions read from the real external sink.
 */
export function makeHarness(t, { concurrency = 4 } = {}) {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'orch-test-'));
  const store = openStore(path.join(tmp, 'test.db'));
  const outboxDir = path.join(tmp, 'outbox');
  const engine = new Engine({
    store,
    runsDir: path.join(tmp, 'runs'),
    concurrency,
    logger: { log() {}, error() {} },
  }).registerAll(createExecutors({ sourceDir: path.join(FIXTURES, 'source'), outboxDir }));

  const harness = {
    tmp,
    store,
    engine,
    outboxDir,
    run(graph) {
      const run = engine.createRun(graph);
      engine.start(run.id);
      return run.id;
    },
    node(runId, nodeId) {
      const row = harness.store.getNodeRun(runId, nodeId);
      return {
        ...row,
        logs: row.logs ? JSON.parse(row.logs) : [],
        effects: harness.store.listEffects(runId).filter((e) => e.node_id === nodeId),
        output: row.outputs ? JSON.parse(row.outputs) : null,
      };
    },
    statuses(runId) {
      return Object.fromEntries(harness.store.listNodeRuns(runId).map((r) => [r.node_id, r.status]));
    },
    get storeRef() {
      return harness.store;
    },
    outboxRecords(target) {
      const file = path.join(outboxDir, `${target}.ndjson`);
      if (!fs.existsSync(file)) return [];
      return fs
        .readFileSync(file, 'utf8')
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line));
    },
    close() {
      harness.engine.close();
      harness.store.close();
    },
  };

  t.after(() => {
    harness.close();
    fs.rmSync(tmp, { recursive: true, force: true });
  });
  return harness;
}

/** Reopens the same SQLite file with a brand-new engine — the restart path. */
export function reboot(harness, { concurrency = 4 } = {}) {
  harness.close();
  const store = openStore(path.join(harness.tmp, 'test.db'));
  const engine = new Engine({
    store,
    runsDir: path.join(harness.tmp, 'runs'),
    concurrency,
    logger: { log() {}, error() {} },
  }).registerAll(
    createExecutors({ sourceDir: path.join(FIXTURES, 'source'), outboxDir: harness.outboxDir }),
  );
  const recovered = engine.recover();
  harness.store = store;
  harness.engine = engine;
  harness.recovered = recovered;
  return { store, engine, recovered };
}
