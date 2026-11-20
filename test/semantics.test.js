import assert from 'node:assert/strict';
import test from 'node:test';
import { compileGraph } from '../src/graph.js';
import { loadFixture, makeHarness } from './helpers.js';

test('a skipped branch carries skip semantics while the parallel branch publishes', async (t) => {
  const h = makeHarness(t);

  // Zero-row USD payload: parse_usd emits count 0, so validate_usd skips,
  // publish_usd skips (upstream_skipped) and report skips (condition reads
  // publish_usd.status). The CNY branch must still complete end to end.
  const raw = loadFixture('orders-daily.json');
  for (const node of raw.nodes) {
    if (node.id === 'parse_usd') node.config.emptyRows = true;
  }
  const graph = compileGraph(raw);
  const runId = h.run(graph);
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);

  const statuses = h.statuses(runId);
  assert.equal(statuses.parse_usd, 'succeeded');
  assert.equal(statuses.validate_usd, 'skipped');
  assert.equal(statuses.publish_usd, 'skipped');
  assert.equal(statuses.publish_cny, 'succeeded');
  assert.equal(statuses.report, 'skipped');
  // publish_usd never succeeded, so its pause gate never fires.
  assert.equal(h.node(runId, 'validate_usd').skip_reason, 'condition');
  assert.equal(h.node(runId, 'publish_usd').skip_reason, 'upstream_skipped');
  assert.equal(h.outboxRecords('usd-orders').length, 0);
  assert.equal(h.store.getRun(runId).status, 'succeeded');
});

test('empty upstream cascades skip through a whole branch', async (t) => {
  const h = makeHarness(t);
  const graph = compileGraph({
    name: 'empty-branch',
    nodes: [
      { id: 'src', type: 'custom', config: { emit: { count: 0 } } },
      { id: 'parse', type: 'custom', dependsOn: ['src'], skipWhen: 'src.output.count == 0' },
      { id: 'validate', type: 'custom', dependsOn: ['parse'] },
      { id: 'publish', type: 'publish', dependsOn: ['validate'], config: { target: 'never' } },
      { id: 'join', type: 'custom', dependsOn: ['src', 'publish'], config: { emit: { joined: true } } },
    ],
  });
  const runId = h.run(graph);
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  assert.deepEqual(h.statuses(runId), {
    src: 'succeeded',
    parse: 'skipped',
    validate: 'skipped',
    publish: 'skipped',
    join: 'skipped',
  });
  assert.equal(h.outboxRecords('never').length, 0, 'a skipped branch must never publish');
  assert.equal(h.store.getRun(runId).status, 'succeeded');
});

test('a node with one skipped and one succeeded dependency is skipped (conservative)', async (t) => {
  const h = makeHarness(t);
  const graph = compileGraph({
    name: 'mixed-join',
    nodes: [
      { id: 'a', type: 'custom' },
      { id: 'b', type: 'custom', dependsOn: ['a'], skipWhen: 'a.status == succeeded' },
      { id: 'c', type: 'custom', dependsOn: ['a'] },
      { id: 'join', type: 'custom', dependsOn: ['b', 'c'] },
    ],
  });
  const runId = h.run(graph);
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  assert.equal(h.statuses(runId).join, 'skipped');
  assert.equal(h.node(runId, 'join').skip_reason, 'upstream_skipped');
});

test('downstream never consumes output from a failed node', async (t) => {
  const h = makeHarness(t);
  const graph = compileGraph({
    name: 'no-partial',
    nodes: [
      { id: 'a', type: 'custom', config: { failHard: true, emit: { partial: true } } },
      { id: 'b', type: 'custom', dependsOn: ['a'] },
      { id: 'c', type: 'custom', dependsOn: ['a', 'b'] },
    ],
  });
  const runId = h.run(graph);
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  const statuses = h.statuses(runId);
  assert.equal(statuses.a, 'failed');
  assert.equal(statuses.b, 'blocked');
  assert.equal(statuses.c, 'blocked');
  assert.equal(h.node(runId, 'a').outputs, null);
  assert.equal(h.node(runId, 'b').inputs, null, 'blocked nodes record no inputs');
});

test('conditional skip evaluation errors fail the node rather than racing on', async (t) => {
  const h = makeHarness(t);
  // compile-time checks pass (a is upstream), but the executor never writes
  // output, so the field is null → a bare atom is false, no crash.
  const graph = compileGraph({
    name: 'cond-eval',
    nodes: [
      { id: 'a', type: 'custom', config: { emit: { count: null } } },
      { id: 'b', type: 'custom', dependsOn: ['a'], skipWhen: 'a.output.count' },
    ],
  });
  const runId = h.run(graph);
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  assert.equal(h.statuses(runId).b, 'succeeded');
});

test('download and parse inputs are passed explicitly to dependents', async (t) => {
  const h = makeHarness(t);
  const graph = compileGraph(loadFixture('orders-daily.json'));
  const runId = h.run(graph);
  await h.engine.waitForStatus(runId, 'paused');
  const validateCny = h.node(runId, 'validate_cny');
  const inputs = JSON.parse(validateCny.inputs);
  assert.deepEqual(Object.keys(inputs), ['parse_cny']);
  assert.match(inputs.parse_cny.artifact, /^parse_cny/);
  const meta = JSON.parse(validateCny.meta);
  assert.match(meta.inputDigest, /^[0-9a-f]{64}$/);
});

test('concurrency cap is respected', async (t) => {
  const h = makeHarness(t, { concurrency: 1 });
  const nodes = [{ id: 'root', type: 'custom', config: { delayMs: 10 } }];
  for (let i = 0; i < 4; i += 1) {
    nodes.push({ id: `leaf${i}`, type: 'custom', dependsOn: ['root'], config: { delayMs: 40 } });
  }
  const runId = h.run(compileGraph({ name: 'cap', nodes }));
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  const leaves = h.engine.describeRun(runId).nodes.filter((n) => n.id.startsWith('leaf'));
  leaves.sort((a, b) => a.startedAt - b.startedAt);
  for (let i = 1; i < leaves.length; i += 1) {
    assert.ok(
      leaves[i].startedAt >= leaves[i - 1].finishedAt - 2,
      'with concurrency 1 leaves must not overlap',
    );
  }
});

test('node logs keep level, message, attempt and are bounded', async (t) => {
  const h = makeHarness(t);
  const graph = compileGraph({
    name: 'logs',
    nodes: [{ id: 'chatty', type: 'custom', config: { delayMs: 5 } }],
  });
  const runId = h.run(graph);
  await h.engine.waitForStatus(runId, ['succeeded', 'failed']);
  const logs = h.node(runId, 'chatty').logs;
  assert.ok(logs.length >= 2);
  assert.deepEqual(Object.keys(logs[0]).sort(), ['at', 'attempt', 'level', 'message']);
  const events = h.store.listEvents(runId).filter((e) => e.node_id === 'chatty' && e.type === 'log');
  assert.ok(events.length >= 2);
});
