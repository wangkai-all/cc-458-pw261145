import assert from 'node:assert/strict';
import test from 'node:test';
import { compileGraph, evaluateCondition, GraphError, parseCondition } from '../src/graph.js';
import { loadFixture } from './helpers.js';

test('orders-daily fixture compiles into a valid DAG with parallel branches', () => {
  const graph = compileGraph(loadFixture('orders-daily.json'));
  assert.equal(graph.nodes.length, 8);
  assert.deepEqual(graph.order[0], 'download');
  assert.ok(graph.order.indexOf('parse_cny') < graph.order.indexOf('validate_cny'));
  assert.ok(graph.order.indexOf('validate_usd') < graph.order.indexOf('report'));
  assert.match(graph.hash, /^[0-9a-f]{64}$/);
});

test('cycle fixture is rejected with the cycle path', () => {
  assert.throws(
    () => compileGraph(loadFixture('cycle.json')),
    (err) => {
      assert.ok(err instanceof GraphError);
      assert.equal(err.code, 'CYCLE');
      assert.deepEqual(err.details.cycle, ['parse', 'validate', 'parse']);
      assert.match(err.message, /循环依赖/);
      return true;
    },
  );
});

test('self dependency is rejected', () => {
  assert.throws(
    () => compileGraph({ name: 'x', nodes: [{ id: 'a', dependsOn: ['a'] }] }),
    (err) => err.code === 'SELF_DEPENDENCY',
  );
});

test('unknown dependency is rejected', () => {
  assert.throws(
    () => compileGraph({ name: 'x', nodes: [{ id: 'a', dependsOn: ['ghost'] }] }),
    (err) => err.code === 'UNKNOWN_DEPENDENCY' && err.details.dependency === 'ghost',
  );
});

test('duplicate node ids are rejected', () => {
  assert.throws(
    () => compileGraph({ name: 'x', nodes: [{ id: 'a' }, { id: 'a' }] }),
    (err) => err.code === 'DUPLICATE_NODE',
  );
});

test('a condition can only reference upstream nodes', () => {
  assert.throws(
    () =>
      compileGraph({
        name: 'x',
        nodes: [
          { id: 'a' },
          { id: 'b', dependsOn: ['a'] },
          { id: 'c', skipWhen: 'b.status == succeeded' },
        ],
      }),
    (err) => err.code === 'INVALID_CONDITION' && err.details.reference === 'b',
  );
});

test('malformed conditions are rejected at compile time', () => {
  for (const bad of ['a.', 'a.status ==', '(a.status', 'a.nope', 'a.status &&']) {
    assert.throws(
      () => compileGraph({ name: 'x', nodes: [{ id: 'a' }, { id: 'b', dependsOn: ['a'], skipWhen: bad }] }),
      (err) => err.code === 'INVALID_CONDITION',
      `expected rejection for ${bad}`,
    );
  }
});

test('condition grammar evaluates status, output and equality', () => {
  const lookup = (id) =>
    ({
      a: { status: 'succeeded', output: { count: 0, kind: 'orders' }, meta: { ok: true } },
      b: { status: 'skipped', output: {} },
    })[id];

  assert.equal(evaluateCondition('a.status == succeeded', lookup), true);
  assert.equal(evaluateCondition('a.status != failed', lookup), true);
  assert.equal(evaluateCondition('a.output.count == 0', lookup), true);
  assert.equal(evaluateCondition('a.output.count', lookup), false);
  assert.equal(evaluateCondition("a.output.kind == 'orders'", lookup), true);
  assert.equal(evaluateCondition('b.status == skipped && a.meta.ok', lookup), true);
  assert.equal(evaluateCondition('!(b.status == succeeded)', lookup), true);
  assert.equal(evaluateCondition('a.status == failed || b.status == skipped', lookup), true);
});

test('precedence: && binds tighter than ||', () => {
  const lookup = () => ({ status: 'succeeded', output: { x: 0 } });
  assert.deepEqual(parseCondition('n.output.x || n.status == succeeded && n.output.y').kind, 'binary');
  assert.equal(
    evaluateCondition('n.status == failed && n.status == succeeded || n.output.x == 0', lookup),
    true,
  );
});

test('graph hash is stable regardless of key order', () => {
  const a = compileGraph({ name: 'x', nodes: [{ id: 'a', config: { p: 1, q: 2 } }] });
  const b = compileGraph({ name: 'x', nodes: [{ id: 'a', config: { q: 2, p: 1 } }] });
  assert.equal(a.hash, b.hash);
});
