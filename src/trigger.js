import fs from 'node:fs';
import path from 'node:path';
import { compileGraph } from './graph.js';
import { readJson } from './util.js';

/**
 * The orchestrator has two trigger sources and both must be crash-safe, so the
 * "trigger" is a small file-backed queue: producers write one intent, the
 * trigger claims it atomically (rename), then hands it to the engine.
 *
 * Trigger file shape (fixtures/trigger.json):
 * {
 *   "graph": "fixtures/graphs/orders-daily.json",
 *   "at": "2026-11-20T09:00:00Z",       // optional: settle before dispatch
 *   "inputs": { ... },                  // optional: surfaced on the run page
 *   "overrides": { "nodeId": { ... } }  // optional: per-run executor config
 * }
 */
export class FileTrigger {
  constructor({ filePath, store, engine, logger = console }) {
    this.filePath = filePath;
    this.store = store;
    this.engine = engine;
    this.logger = logger;
    this.timer = null;
    this.seen = new Set();
    this.onRun = null;
  }

  start(intervalMs = 500) {
    const tick = async () => {
      try {
        await this.poll();
      } catch (err) {
        this.logger.error?.('触发器轮询失败：', err.message);
      }
    };
    tick();
    this.timer = setInterval(tick, intervalMs);
    this.timer.unref?.();
    return this;
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  async poll() {
    const stats = await fs.promises.stat(this.filePath).catch(() => null);
    if (!stats) return null;
    const stamp = `${stats.mtimeMs}:${stats.size}`;
    if (this.seen.has(stamp)) return null;
    this.seen.add(stamp);

    const claim = `${this.filePath}.claim-${process.pid}-${Date.now()}`;
    try {
      await fs.promises.rename(this.filePath, claim);
    } catch {
      return null;
    }
    let spec;
    try {
      spec = readJson(claim);
    } catch (err) {
      this.logger.error?.('触发器文件无法解析：', err.message);
      await fs.promises.rm(claim, { force: true });
      return null;
    }
    await fs.promises.rm(claim, { force: true });

    if (spec.at && Date.parse(spec.at) > Date.now()) {
      // not due yet — put it back for the next poll
      await fs.promises.writeFile(this.filePath, JSON.stringify(spec, null, 2), 'utf8');
      this.seen.delete(stamp);
      return null;
    }

    const raw = readJson(path.resolve(spec.graph));
    const overrides = spec.overrides && typeof spec.overrides === 'object' ? spec.overrides : {};
    // Per-run executor config overrides (e.g. inject a failure in a demo) are
    // merged before compilation so the stored definition matches what ran.
    for (const node of raw.nodes ?? []) {
      const override = overrides[node.id];
      if (!override || typeof override !== 'object') continue;
      node.config = { ...(node.config ?? {}), ...override };
    }
    let graph;
    try {
      graph = compileGraph(raw);
    } catch (err) {
      this.logger.error?.(`触发失败：${err.message}`);
      return { error: err };
    }
    const snapshot = {
      trigger: {
        at: spec.at ?? new Date().toISOString(),
        inputs: spec.inputs ?? {},
        overrides,
      },
    };
    const run = this.engine.createRun(graph);
    this.store.updateRun(run.id, { snapshot: JSON.stringify(snapshot) });
    this.engine.start(run.id);
    this.onRun?.(run.id);
    this.logger.log?.(`触发器已派发运行 ${run.id}`);
    return { runId: run.id };
  }
}
