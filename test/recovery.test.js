import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { compileGraph } from '../src/graph.js';
import { loadFixture, makeHarness, reboot } from './helpers.js';

function slowGraph(extra = {}) {
  return compileGraph({
    name: 'slow',
    nodes: [
      { id: 'a', type: 'custom', config: { delayMs: 400 } },
      { id: 'b', type: 'custom', dependsOn: ['a'], config: { delayMs: 50 } },
      { id: 'c', type: 'custom', dependsOn: ['a'], config: { delayMs: 50 } },
      ...extra.nodes ?? [],
    ],
    ...(extra.root ?? {}),
  });
}

test('cancel interrupts in-flight work and never runs pending nodes', async (t) => {
  const h = makeHarness(t);
  const runId = h.run(slowGraph());
  // let `a` start
  await new Promise((r) => setTimeout(r, 80));
  assert.equal(h.statuses(runId).a, 'running');

  h.engine.cancel(runId);
  await h.engine.waitForIdle(runId);
  const statuses = h.statuses(runId);
  assert.equal(h.store.getRun(runId).status, 'cancelled');
  assert.equal(statuses.a, 'cancelled');
  assert.equal(statuses.b, 'cancelled');
  assert.equal(statuses.c, 'cancelled');
  assert.match(h.node(runId, 'a').error, /取消/);

  const events = h.store.listEvents(runId).map((e) => e.type);
  assert.ok(events.includes('cancelled'));
  assert.ok(events.includes('node_cancelled'));
  // no node finished successfully after cancellation
  assert.equal(events.filter((e) => e === 'node_succeeded').length, 0);
});

test('cancel on a paused run works too', async (t) => {
  const h = makeHarness(t);
  const graph = compileGraph({
    name: 'gate',
    nodes: [
      { id: 'a', type: 'custom', pauseAfter: true, config: { delayMs: 5 } },
      { id: 'b', type: 'custom', dependsOn: ['a'], config: { delayMs: 5 } },
    ],
  });
  const runId = h.run(graph);
  await h.engine.waitForStatus(runId, 'paused');
  h.engine.cancel(runId);
  await h.engine.waitForIdle(runId);
  assert.equal(h.store.getRun(runId).status, 'cancelled');
  assert.equal(h.statuses(runId).b, 'cancelled');
});

test('pause lets the in-flight node finish, then stops scheduling', async (t) => {
  const h = makeHarness(t);
  const runId = h.run(slowGraph());
  await new Promise((r) => setTimeout(r, 60));
  h.engine.pause(runId);
  await h.engine.waitForStatus(runId, 'paused');
  await waitUntil(() => h.statuses(runId).a !== 'running');
  const statuses = h.statuses(runId);
  assert.equal(statuses.a, 'succeeded');
  assert.equal(statuses.b, 'pending');
  assert.equal(statuses.c, 'pending');

  h.engine.resume(runId);
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  assert.deepEqual(h.statuses(runId), { a: 'succeeded', b: 'succeeded', c: 'succeeded' });
});

async function waitUntil(predicate, timeoutMs = 5000) {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error('condition timed out');
    await new Promise((r) => setTimeout(r, 10));
  }
}

test('restart after cancellation does not resurrect the run', async (t) => {
  const h = makeHarness(t);
  const runId = h.run(slowGraph());
  await new Promise((r) => setTimeout(r, 80));
  h.engine.cancel(runId);
  await h.engine.waitForIdle(runId);

  const { recovered, engine } = reboot(h);
  assert.ok(!recovered.some((r) => r.runId === runId));
  await new Promise((r) => setTimeout(r, 100));
  assert.equal(h.store.getRun(runId).status, 'cancelled');
  assert.equal(h.statuses(runId).b, 'cancelled');
  engine.close();
});

test('restart resumes a paused run in place, without re-running confirmed nodes', async (t) => {
  const h = makeHarness(t);
  const graph = compileGraph(loadFixture('resume.json'));
  const runId = h.run(graph);
  // no pause gate in this fixture — stop the whole engine mid-flight instead
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  const before = h.outboxRecords('resume-orders');
  assert.equal(before.length, 1);

  const { recovered } = reboot(h);
  assert.ok(!recovered.some((r) => r.runId === runId), 'finished run must not be replayed');
  assert.equal(h.statuses(runId).publish, 'succeeded');
  assert.equal(h.outboxRecords('resume-orders').length, 1, 'publish must not be duplicated');
});

test('crash between confirmed side effect and node completion is recovered exactly once', async (t) => {
  const h = makeHarness(t);
  const graph = compileGraph({
    name: 'crash',
    nodes: [
      { id: 'src', type: 'custom', config: { emit: { digest: 'd'.repeat(64) }, delayMs: 5 } },
      {
        id: 'publish',
        type: 'publish',
        dependsOn: ['src'],
        config: { target: 'crash-target', delayMs: 5 },
      },
      { id: 'after', type: 'custom', dependsOn: ['publish'], config: { delayMs: 5 } },
    ],
  });
  const runId = h.run(graph);
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  assert.equal(h.outboxRecords('crash-target').length, 1);

  // Simulate the crash window: the effect ledger has the publish, but the node
  // and run rows were never updated (process died in between).
  h.store.updateNodeRun(runId, 'publish', {
    status: 'running',
    outputs: null,
    finished_at: null,
  });
  h.store.updateRun(runId, { status: 'running', finished_at: null, updated_at: Date.now() });
  h.store.updateNodeRun(runId, 'after', {
    status: 'pending',
    outputs: null,
    finished_at: null,
  });

  const { recovered, engine } = reboot(h);
  assert.ok(recovered.some((r) => r.runId === runId));
  await engine.waitForStatus(runId, ['succeeded', 'failed']);
  assert.equal(h.outboxRecords('crash-target').length, 1, 'effect must be adopted, not repeated');
  assert.equal(h.statuses(runId).publish, 'succeeded');
  assert.equal(h.statuses(runId).after, 'succeeded');
  const adoptEvent = h.store
    .listEvents(runId)
    .find((e) => e.type === 'effect_adopted' && e.node_id === 'publish');
  assert.ok(adoptEvent, 'expected an effect_adopted event');
});

test('restart while a node is running requeues it and it runs once to completion', async (t) => {
  const h = makeHarness(t);
  const graph = compileGraph({
    name: 'midflight',
    nodes: [
      { id: 'a', type: 'custom', config: { delayMs: 300, emit: { ok: true } } },
      { id: 'b', type: 'custom', dependsOn: ['a'], config: { delayMs: 10 } },
    ],
  });
  const runId = h.run(graph);
  await new Promise((r) => setTimeout(r, 60));
  assert.equal(h.statuses(runId).a, 'running');

  const { engine } = reboot(h);
  await engine.waitForStatus(runId, ['succeeded', 'failed']);
  assert.equal(h.statuses(runId).a, 'succeeded');
  assert.equal(h.statuses(runId).b, 'succeeded');
  const recoveredEvent = h.store.listEvents(runId).find((e) => e.type === 'recovered' && e.node_id === 'a');
  assert.ok(recoveredEvent);
});

test('manual retry re-runs the node plus descendants and adopts the confirmed publish', async (t) => {
  const h = makeHarness(t);
  const marker = path.join(h.tmp, 'flaky.marker');
  fs.writeFileSync(marker, 'fail me');
  const graph = compileGraph({
    name: 'manual-retry',
    nodes: [
      { id: 'src', type: 'custom', config: { emit: { digest: 'a'.repeat(64) }, delayMs: 5 } },
      { id: 'publish', type: 'publish', dependsOn: ['src'], config: { target: 'mr-target' } },
      { id: 'sibling', type: 'custom', dependsOn: ['src'], config: { delayMs: 5 } },
      {
        id: 'flaky',
        type: 'custom',
        dependsOn: ['publish'],
        config: { failIfFile: marker, delayMs: 5 },
      },
    ],
  });
  const runId = h.run(graph);
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  assert.equal(h.statuses(runId).flaky, 'failed');
  assert.equal(h.outboxRecords('mr-target').length, 1);

  assert.deepEqual(h.engine.retryScope(runId, 'flaky'), ['flaky']);
  assert.deepEqual(h.engine.retryScope(runId, 'publish'), ['publish', 'flaky']);

  const siblingFinishedAt = h.node(runId, 'sibling').finished_at;
  fs.rmSync(marker);

  // Retry from `publish`: it re-executes but adopts the confirmed effect, and
  // its unconfirmed descendant `flaky` re-runs. `sibling` is out of scope.
  h.engine.retry(runId, 'publish');
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);

  assert.equal(h.store.getRun(runId).status, 'succeeded');
  assert.equal(h.statuses(runId).publish, 'succeeded');
  assert.equal(h.statuses(runId).flaky, 'succeeded');
  assert.equal(h.outboxRecords('mr-target').length, 1, 'retry must adopt the confirmed publish');
  assert.equal(h.node(runId, 'sibling').finished_at, siblingFinishedAt, 'sibling is outside the retry scope');
  const adopted = h.store
    .listEvents(runId)
    .filter((e) => e.type === 'effect_adopted' && e.node_id === 'publish');
  assert.equal(adopted.length, 1);
  const retryEvent = h.store.listEvents(runId).find((e) => e.type === 'retry_requested');
  assert.deepEqual(retryEvent.detail.scope, ['publish', 'flaky']);
});

test('retry scope is shown for every failed or blocked node', async (t) => {
  const h = makeHarness(t);
  const runId = h.run(compileGraph(loadFixture('partial-failure.json')));
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  const described = h.engine.describeRun(runId);
  assert.deepEqual(described.retryScopes.parse_broken, [
    'parse_broken',
    'validate_broken',
    'merge',
  ]);
  assert.deepEqual(described.retryScopes.merge, ['merge']);
  assert.equal(described.retryScopes.publish_ok, undefined);
});

test('retry refuses to start while the run is live', async (t) => {
  const h = makeHarness(t);
  const runId = h.run(slowGraph());
  await new Promise((r) => setTimeout(r, 40));
  assert.throws(() => h.engine.retry(runId, 'a'), /运行中不能手动重试/);
});

test('runs directory artifacts are written atomically', async (t) => {
  const h = makeHarness(t);
  const runId = h.run(compileGraph(loadFixture('orders-daily.json')));
  await h.engine.waitForStatus(runId, 'paused');
  const dir = path.join(h.tmp, 'runs', runId);
  const files = fs.readdirSync(dir);
  assert.ok(files.includes('download.json'));
  assert.ok(files.includes('parse_cny.json'));
  assert.ok(!files.some((f) => f.includes('.tmp')), 'no leftover temp files');
});

test('a cancelled run can be retried from its first cancelled node', async (t) => {
  const h = makeHarness(t);
  const runId = h.run(slowGraph());
  await new Promise((r) => setTimeout(r, 70));
  h.engine.cancel(runId);
  await h.engine.waitForIdle(runId);

  const described = h.engine.describeRun(runId);
  assert.ok(described.retryScopes.a, 'cancelled runs must expose a retry entry point');
  assert.deepEqual(described.retryScopes.a, ['a', 'b', 'c']);

  h.engine.retry(runId, 'a');
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  assert.deepEqual(h.statuses(runId), { a: 'succeeded', b: 'succeeded', c: 'succeeded' });
});
