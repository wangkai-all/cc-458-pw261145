import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { createApp } from '../src/app.js';
import { FIXTURES, ROOT } from './helpers.js';

export async function bootApp(t, { dataDir, triggerPollMs = 60, concurrency = 4 } = {}) {
  const dir = dataDir ?? fs.mkdtempSync(path.join(process.env.TMPDIR ?? '/tmp', 'orch-app-'));
  const app = createApp({
    rootDir: ROOT,
    dataDir: dir,
    sourceDir: path.join(FIXTURES, 'source'),
    concurrency,
    triggerPollMs,
    logger: { log() {}, error() {} },
  });
  await new Promise((resolve) => app.server.listen(0, '127.0.0.1', resolve));
  const { port } = app.server.address();
  const base = `http://127.0.0.1:${port}`;

  const api = async (method, p, body) => {
    const res = await fetch(`${base}${p}`, {
      method,
      headers: body ? { 'content-type': 'application/json' } : undefined,
      body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch {
      json = text;
    }
    return { status: res.status, body: json };
  };

  t.after(async () => {
    await app.close();
    if (!dataDir) fs.rmSync(dir, { recursive: true, force: true });
  });

  return { app, base, api, dataDir: dir };
}

export async function waitFor(fn, { timeoutMs = 8000, intervalMs = 20 } = {}) {
  const started = Date.now();
  for (;;) {
    const value = await fn();
    if (value) return value;
    if (Date.now() - started > timeoutMs) throw new Error('waitFor timeout');
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

test('API: fixtures list reports the cycle fixture as invalid', async (t) => {
  const { api } = await bootApp(t);
  const res = await api('GET', '/api/fixtures');
  assert.equal(res.status, 200);
  const cycle = res.body.find((f) => f.file === 'cycle.json');
  assert.equal(cycle.validation.ok, false);
  assert.equal(cycle.validation.code, 'CYCLE');
  assert.ok(cycle.validation.details.cycle.length >= 2);
  const good = res.body.find((f) => f.file === 'orders-daily.json');
  assert.equal(good.validation.ok, true);
});

test('API: creating a run from the cycle fixture returns 422 and writes nothing', async (t) => {
  const { api, app } = await bootApp(t);
  const res = await api('POST', '/api/runs', { fixture: 'cycle.json' });
  assert.equal(res.status, 422);
  assert.equal(res.body.code, 'CYCLE');
  assert.equal(app.store.listRuns().length, 0);
});

test('API: fixture names cannot escape the fixture directory', async (t) => {
  const { api } = await bootApp(t);
  for (const name of ['../package.json', '..%2Fpackage.json', 'nested/orders-daily.json']) {
    const res = await api('POST', '/api/runs', { fixture: name });
    assert.equal(res.status, 404, `expected 404 for ${name}`);
  }
});

test('API: full run over HTTP with pause and resume', async (t) => {
  const { api } = await bootApp(t);
  const created = await api('POST', '/api/runs', { fixture: 'orders-daily.json' });
  assert.equal(created.status, 201);
  const runId = created.body.runId;

  await waitFor(async () => (await api('GET', `/api/runs/${runId}`)).body.run.status === 'paused');
  const paused = (await api('GET', `/api/runs/${runId}`)).body;
  assert.equal(paused.run.gate, 'pause_after:publish_usd');
  assert.ok(paused.nodes.every((n) => n.logs.length >= 0));
  const publishUsd = paused.nodes.find((n) => n.id === 'publish_usd');
  assert.equal(publishUsd.status, 'succeeded');
  assert.equal(publishUsd.effects.length, 1);
  assert.match(publishUsd.effects[0].effect_key, /^publish:usd-orders:/);

  const resumed = await api('POST', `/api/runs/${runId}/resume`);
  assert.equal(resumed.status, 200);
  const done = await waitFor(async () => {
    const r = (await api('GET', `/api/runs/${runId}`)).body;
    return r.run.status === 'succeeded' ? r : null;
  });
  assert.equal(done.nodes.find((n) => n.id === 'report').status, 'succeeded');
  assert.ok(done.events.some((e) => e.type === 'run_succeeded'));
});

test('API: SSE stream replays snapshot and closes on terminal status', async (t) => {
  const { api, base } = await bootApp(t);
  const created = await api('POST', '/api/runs', { fixture: 'resume.json' });
  const runId = created.body.runId;
  await waitFor(async () => (await api('GET', `/api/runs/${runId}`)).body.run.status === 'succeeded');

  const res = await fetch(`${base}/api/runs/${runId}/stream`);
  assert.equal(res.status, 200);
  assert.match(res.headers.get('content-type'), /text\/event-stream/);
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let text = '';
  const deadline = Date.now() + 4000;
  while (Date.now() < deadline) {
    const { value, done } = await reader.read();
    if (done) break;
    text += decoder.decode(value, { stream: true });
    if (text.includes('event: closed')) break;
  }
  await reader.cancel().catch(() => {});
  assert.match(text, /event: snapshot/);
  assert.match(text, /event: closed/);
  assert.match(text, /"run_succeeded"/);
});

test('API: retry-scope endpoint mirrors the engine scope for failed nodes', async (t) => {
  const { api } = await bootApp(t);
  const created = await api('POST', '/api/runs', { fixture: 'partial-failure.json' });
  const runId = created.body.runId;
  await waitFor(async () => {
    const r = (await api('GET', `/api/runs/${runId}`)).body;
    return r.run.status === 'failed' ? r : null;
  });
  const scope = await api('GET', `/api/runs/${runId}/retry-scope?nodeId=parse_broken`);
  assert.equal(scope.status, 200);
  assert.deepEqual(scope.body.scope, ['parse_broken', 'validate_broken', 'merge']);
});

test('API: retry rejects a live run with 409 but accepts a cancelled one', async (t) => {
  const { api } = await bootApp(t, { concurrency: 1 });
  const created = await api('POST', '/api/runs', {
    definition: {
      name: 'slow',
      nodes: [{ id: 'a', type: 'custom', delayMs: 500, config: { delayMs: 500 } }],
    },
  });
  const runId = created.body.runId;
  await new Promise((r) => setTimeout(r, 40));

  const live = await api('POST', `/api/runs/${runId}/retry`, { nodeId: 'a' });
  assert.equal(live.status, 409);

  await api('POST', `/api/runs/${runId}/cancel`);
  const afterCancel = await api('POST', `/api/runs/${runId}/retry`, { nodeId: 'a' });
  assert.equal(afterCancel.status, 202);
  assert.deepEqual(afterCancel.body.scope, ['a']);
  await waitFor(async () => (await api('GET', `/api/runs/${runId}`)).body.run.status === 'succeeded');
});

test('API: pause/cancel validate state transitions', async (t) => {
  const { api } = await bootApp(t);
  const created = await api('POST', '/api/runs', { fixture: 'resume.json' });
  const runId = created.body.runId;
  await waitFor(async () => (await api('GET', `/api/runs/${runId}`)).body.run.status === 'succeeded');
  assert.equal((await api('POST', `/api/runs/${runId}/pause`)).status, 409);
  assert.equal((await api('POST', `/api/runs/${runId}/resume`)).status, 409);
  assert.equal((await api('POST', `/api/runs/${runId}/cancel`)).status, 409);
  assert.equal((await api('GET', '/api/runs/nope')).status, 404);
});

test('API: trigger endpoint dispatches exactly one run per trigger file', async (t) => {
  const { api, app } = await bootApp(t, { triggerPollMs: 30 });
  const res = await api('POST', '/api/trigger', {
    fixture: 'orders-daily.json',
    inputs: { window: '2026-11-20' },
  });
  assert.equal(res.status, 202);
  assert.ok(res.body.dispatched.runId);
  await new Promise((r) => setTimeout(r, 200));
  assert.equal(app.store.listRuns().length, 1);
});

test('API: restart recovers an interrupted run through the real boot path', async (t) => {
  const tmpDir = fs.mkdtempSync(path.join(process.env.TMPDIR ?? '/tmp', 'orch-restart-'));
  t.after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  const first = await bootApp(t, { dataDir: tmpDir });
  const created = await first.api('POST', '/api/runs', {
    definition: {
      name: 'interruptible',
      nodes: [
        {
          id: 'download',
          type: 'download',
          config: { source: 'orders', delayMs: 250 },
        },
        {
          id: 'parse',
          type: 'parse',
          dependsOn: ['download'],
          config: { delayMs: 5 },
        },
        {
          id: 'validate',
          type: 'validate',
          dependsOn: ['parse'],
          config: { minRows: 1 },
        },
        {
          id: 'publish',
          type: 'publish',
          dependsOn: ['validate'],
          config: { target: 'restart-target' },
        },
        { id: 'z', type: 'custom', dependsOn: ['publish'] },
      ],
    },
  });
  const runId = created.body.runId;
  await new Promise((r) => setTimeout(r, 90));
  await first.app.close();

  const second = await bootApp(t, { dataDir: tmpDir });
  assert.ok(second.app.recovered.some((r) => r.runId === runId));
  await waitFor(async () => {
    const r = (await second.api('GET', `/api/runs/${runId}`)).body;
    return r.run.status === 'succeeded' ? r : null;
  });
  const outbox = path.join(tmpDir, 'outbox', 'restart-target.ndjson');
  assert.equal(fs.readFileSync(outbox, 'utf8').trim().split('\n').length, 1);
});

test('API: trigger overrides are applied to the run definition', async (t) => {
  const { api, app } = await bootApp(t, { triggerPollMs: 30 });
  const res = await api('POST', '/api/trigger', {
    fixture: 'orders-daily.json',
    overrides: { parse_usd: { emptyRows: true } },
  });
  assert.equal(res.status, 202);
  const runId = res.body.dispatched.runId;
  await waitFor(async () => {
    const r = (await api('GET', `/api/runs/${runId}`)).body;
    return ['succeeded', 'failed', 'paused'].includes(r.run.status) ? r : null;
  });
  const detail = (await api('GET', `/api/runs/${runId}`)).body;
  const parseUsd = detail.graph.nodes.find((n) => n.id === 'parse_usd');
  assert.equal(parseUsd.config.emptyRows, true);
  // zero rows -> validate_usd skips -> publish_usd skips -> no pause gate
  assert.equal(detail.nodes.find((n) => n.id === 'validate_usd').status, 'skipped');
  assert.equal(detail.nodes.find((n) => n.id === 'publish_usd').status, 'skipped');
  assert.equal(app.store.listRuns().length, 1);
});
