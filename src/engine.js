import { evaluateCondition } from './graph.js';
import { abortError, isAbortError, parseJson, sha256, stableJson } from './util.js';

export const NODE_STATUS = {
  PENDING: 'pending',
  RUNNING: 'running',
  SUCCEEDED: 'succeeded',
  FAILED: 'failed',
  SKIPPED: 'skipped',
  BLOCKED: 'blocked',
  CANCELLED: 'cancelled',
};

const TERMINAL = new Set(['succeeded', 'failed', 'skipped', 'blocked', 'cancelled']);
const RUN_TERMINAL = new Set(['succeeded', 'failed', 'cancelled']);

export function isTerminal(status) {
  return TERMINAL.has(status);
}

/**
 * Executes a persisted DAG run.
 *
 * Durability contract:
 *  - status transitions and the side-effect ledger live in SQLite; a process
 *    restart only replays nodes that were not confirmed complete.
 *  - executors call ctx.effect(key, meta, fn) for external writes. A committed
 *    key is never executed twice; the executor adopts the recorded result.
 *  - a node runs only when every dependency has succeeded (`skipped` propagates,
 *    any bad dependency blocks), so downstream never consumes a partial input.
 */
export class Engine {
  constructor({ store, runsDir, concurrency = 4, logger = console }) {
    this.store = store;
    this.runsDir = runsDir;
    this.concurrency = concurrency;
    this.logger = logger;
    this.executors = new Map();
    this.runtimes = new Map();
    this.tasks = new Set();
    this.closed = false;
  }

  register(type, executor) {
    this.executors.set(type, executor);
    return this;
  }

  registerAll(map) {
    for (const [type, executor] of Object.entries(map)) this.register(type, executor);
    return this;
  }

  // ---- lifecycle ----------------------------------------------------------

  /**
   * Loads every run that was live when the process stopped. In-flight nodes are
   * reset to pending (never marked succeeded on faith); confirmed effects stay
   * in the ledger. Paused runs stay paused and are the recovery entry point.
   */
  recover() {
    const recovered = [];
    for (const row of this.store.listLiveRuns()) {
      const graph = JSON.parse(row.definition);
      const nodes = this.store.listNodeRuns(row.id);
      for (const node of nodes) {
        if (node.status === NODE_STATUS.RUNNING) {
          this.store.updateNodeRun(row.id, node.node_id, {
            status: NODE_STATUS.PENDING,
            updated_at: Date.now(),
          });
          this.store.appendEvent(
            row.id,
            node.node_id,
            'recovered',
            '进程中断时该节点仍在运行，已重新排队；已确认的副作用不会重复执行',
          );
        }
      }
      const rt = this.#ensureRuntime(row.id, graph);
      if (row.status === 'running') {
        this.store.appendEvent(row.id, null, 'recovered', '实例恢复：继续未完成的运行');
        this.#schedule(rt);
      } else {
        this.store.appendEvent(row.id, null, 'recovered', '实例恢复：运行处于人工暂停，等待恢复');
      }
      recovered.push({ runId: row.id, status: row.status });
    }
    return recovered;
  }

  createRun(graph, { status = 'running', id } = {}) {
    const runId = id || `run_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`;
    this.store.upsertPipeline(graph);
    const run = this.store.createRun({ id: runId, graph, status });
    this.store.appendEvent(runId, null, 'run_created', `创建运行：${graph.name}`);
    return run;
  }

  start(runId) {
    const run = this.store.getRun(runId);
    if (!run) throw new Error(`运行 ${runId} 不存在`);
    const rt = this.#ensureRuntime(runId, JSON.parse(run.definition));
    this.#schedule(rt);
    return rt;
  }

  pause(runId) {
    const run = this.store.getRun(runId);
    if (!run) throw new EngineError('NOT_FOUND', '运行不存在');
    if (run.status !== 'running') throw new EngineError('INVALID_STATE', '只有运行中的任务可以暂停');
    this.store.updateRun(runId, { status: 'paused', gate: 'paused', updated_at: Date.now() });
    this.store.appendEvent(runId, null, 'paused', '人工暂停：不再调度新节点，运行中的节点先完成');
    const rt = this.runtimes.get(runId);
    if (rt) rt.paused = true;
    return this.store.getRun(runId);
  }

  resume(runId) {
    const run = this.store.getRun(runId);
    if (!run) throw new EngineError('NOT_FOUND', '运行不存在');
    if (run.status !== 'paused') throw new EngineError('INVALID_STATE', '只有已暂停的任务可以恢复');
    this.store.updateRun(runId, { status: 'running', gate: null, updated_at: Date.now() });
    this.store.appendEvent(runId, null, 'resumed', '人工恢复：继续调度待执行节点');
    const rt = this.#ensureRuntime(runId, JSON.parse(run.definition));
    rt.paused = false;
    this.#schedule(rt);
    return this.store.getRun(runId);
  }

  cancel(runId) {
    const run = this.store.getRun(runId);
    if (!run) throw new EngineError('NOT_FOUND', '运行不存在');
    if (!['running', 'paused'].includes(run.status)) {
      throw new EngineError('INVALID_STATE', '只有运行中或已暂停的任务可以取消');
    }
    const rt = this.runtimes.get(runId);
    const now = Date.now();
    if (rt) {
      rt.cancelled = true;
      rt.paused = true;
      for (const controller of rt.active.values()) controller.abort();
    }
    const nodes = this.store.listNodeRuns(runId);
    for (const node of nodes) {
      if (node.status === NODE_STATUS.PENDING) {
        this.store.updateNodeRun(runId, node.node_id, {
          status: NODE_STATUS.CANCELLED,
          error: '运行已取消（节点未开始）',
          finished_at: now,
          updated_at: now,
        });
      }
    }
    this.store.updateRun(runId, {
      status: 'cancelled',
      gate: null,
      cancel_requested: 1,
      finished_at: now,
      updated_at: now,
    });
    this.store.appendEvent(runId, null, 'cancelled', '人工取消：进行中的节点已中断，未开始节点不再执行');
    this.#finishRuntime(runId);
    return this.store.getRun(runId);
  }

  /**
   * Manual retry. The scope is the node plus every descendant, because a
   * downstream result computed from an old upstream result is stale. Nodes in
   * the range that already succeeded are re-executed; confirmed side effects
   * are adopted rather than repeated.
   */
  retry(runId, nodeId) {
    const run = this.store.getRun(runId);
    if (!run) throw new EngineError('NOT_FOUND', '运行不存在');
    if (run.status === 'running') throw new EngineError('INVALID_STATE', '运行中不能手动重试，请先暂停或取消');
    const graph = JSON.parse(run.definition);
    const target = graph.nodes.find((n) => n.id === nodeId);
    if (!target) throw new EngineError('NOT_FOUND', '节点不存在');
    const scope = this.retryScope(runId, nodeId);
    if (!scope.length) throw new EngineError('INVALID_STATE', '重试范围为空');

    const now = Date.now();
    for (const id of scope) {
      const nodeRun = this.store.getNodeRun(runId, id);
      if (!nodeRun || nodeRun.status === NODE_STATUS.RUNNING) continue;
      this.store.updateNodeRun(runId, id, {
        status: NODE_STATUS.PENDING,
        attempts: 0,
        outputs: null,
        inputs: null,
        meta: null,
        error: null,
        skip_reason: null,
        started_at: null,
        finished_at: null,
        updated_at: now,
      });
    }
    this.store.updateRun(runId, {
      status: 'running',
      gate: null,
      cancel_requested: 0,
      finished_at: null,
      updated_at: now,
    });
    this.store.appendEvent(runId, nodeId, 'retry_requested', `人工重试：重跑范围 ${scope.join(' → ')}`, {
      scope,
    });
    const rt = this.#ensureRuntime(runId, graph);
    rt.paused = false;
    rt.cancelled = false;
    this.#schedule(rt);
    return { run: this.store.getRun(runId), scope };
  }

  /** Descendants of nodeId, including itself; presentation ordered topologically. */
  retryScope(runId, nodeId) {
    const run = this.store.getRun(runId);
    if (!run) throw new EngineError('NOT_FOUND', '运行不存在');
    const graph = JSON.parse(run.definition);
    const inScope = new Set([nodeId]);
    let changed = true;
    while (changed) {
      changed = false;
      for (const node of graph.nodes) {
        if (inScope.has(node.id)) continue;
        if (node.dependsOn.some((dep) => inScope.has(dep))) {
          inScope.add(node.id);
          changed = true;
        }
      }
    }
    return graph.order.filter((id) => inScope.has(id));
  }

  describeRun(runId) {
    const run = this.store.getRun(runId);
    if (!run) return null;
    const graph = JSON.parse(run.definition);
    const nodeRows = this.store.listNodeRuns(runId);
    const effects = this.store.listEffects(runId);
    const nodes = nodeRows.map((row) => this.#presentNode(runId, row, graph, effects));
    const retryScopes = {};
    for (const node of nodes) {
      if ([NODE_STATUS.FAILED, NODE_STATUS.BLOCKED].includes(node.status)) {
        retryScopes[node.id] = this.retryScope(runId, node.id);
      }
    }
    // A cancelled run can still be retried; expose the scope from its first
    // cancelled node so the UI can offer a recovery entry point.
    if (run.status === 'cancelled') {
      const first = nodes.find((n) => n.status === NODE_STATUS.CANCELLED);
      if (first) retryScopes[first.id] = this.retryScope(runId, first.id);
    }
    return {
      run: {
        ...run,
        definition: undefined,
        nodeCount: nodes.length,
        counts: countBy(nodes, 'status'),
      },
      graph,
      nodes,
      events: this.store.listEvents(runId, 0, 1000),
      effects,
      retryScopes,
    };
  }

  // ---- scheduling ---------------------------------------------------------

  #ensureRuntime(runId, graph) {
    let rt = this.runtimes.get(runId);
    if (rt) return rt;
    const run = this.store.getRun(runId);
    rt = {
      runId,
      graph,
      nodes: new Map(graph.nodes.map((n) => [n.id, n])),
      active: new Map(),
      paused: run?.status === 'paused',
      cancelled: run?.status === 'cancelled',
      tickQueued: false,
      finishing: false,
    };
    this.runtimes.set(runId, rt);
    return rt;
  }

  #schedule(rt) {
    if (this.closed) return;
    const run = this.store.getRun(rt.runId);
    if (!run || RUN_TERMINAL.has(run.status) || run.status === 'paused') return;
    if (rt.tickQueued) return;
    rt.tickQueued = true;
    setImmediate(() => {
      rt.tickQueued = false;
      this.#tick(rt).catch((err) => {
        this.logger.error?.(`调度失败 ${rt.runId}:`, err);
        this.store.appendEvent(rt.runId, null, 'scheduler_error', `调度异常：${err.message}`);
      });
    });
  }

  async #tick(rt) {
    if (this.closed) return;
    const run = this.store.getRun(rt.runId);
    if (!run || RUN_TERMINAL.has(run.status) || run.status === 'paused') return;

    const rows = this.store.listNodeRuns(rt.runId);
    const byId = new Map(rows.map((row) => [row.node_id, row]));
    let changed = false;

    for (const graphNode of rt.graph.nodes) {
      const row = byId.get(graphNode.id);
      if (!row || row.status !== NODE_STATUS.PENDING) continue;
      const deps = graphNode.dependsOn.map((dep) => byId.get(dep));

      // A bad dependency can never be consumed: block immediately.
      const badDep = deps.find((dep) =>
        [NODE_STATUS.FAILED, NODE_STATUS.CANCELLED, NODE_STATUS.BLOCKED].includes(dep?.status),
      );
      if (badDep) {
        this.#blockNode(rt, graphNode, badDep);
        changed = true;
        continue;
      }
      if (deps.some((dep) => dep.status === NODE_STATUS.PENDING || dep.status === NODE_STATUS.RUNNING)) {
        continue;
      }

      // All dependencies are terminal and none is bad.
      if (deps.some((dep) => dep.status === NODE_STATUS.SKIPPED)) {
        this.#skipNode(rt, graphNode, 'upstream_skipped', '上游分支已跳过，节点不再执行');
        changed = true;
        continue;
      }
      if (deps.some((dep) => dep.status !== NODE_STATUS.SUCCEEDED)) continue;

      if (graphNode.skipWhen) {
        const snapshots = this.#snapshots(rt.runId);
        let decision;
        try {
          decision = evaluateCondition(graphNode.skipWhen, (id) => snapshots.get(id));
        } catch (err) {
          this.#failNode(rt, graphNode, `跳过条件求值失败：${err.message}`);
          changed = true;
          continue;
        }
        if (decision) {
          this.#skipNode(rt, graphNode, 'condition', `满足跳过条件：${graphNode.skipWhen}`);
          changed = true;
          continue;
        }
      }

      if (rt.active.size >= this.concurrency) continue;
      const inputs = {};
      for (const dep of deps) {
        const output = parseJson(dep.outputs, {});
        if (output == null) {
          this.#blockNode(rt, graphNode, dep, '上游输出缺失，拒绝消费不完整输入');
          changed = true;
          break;
        }
        inputs[dep.node_id] = output;
      }
      if (!byId.has(graphNode.id)) continue;
      const refreshed = this.store.getNodeRun(rt.runId, graphNode.id);
      if (refreshed?.status !== NODE_STATUS.PENDING) continue;
      this.#launch(rt, graphNode, inputs, byId);
      changed = true;
    }

    if (changed) this.#schedule(rt);
    this.#maybeFinalize(rt);
  }

  #launch(rt, graphNode, inputs, byId) {
    const attempt = (byId.get(graphNode.id)?.attempts ?? 0) + 1;
    const now = Date.now();
    this.store.updateNodeRun(rt.runId, graphNode.id, {
      status: NODE_STATUS.RUNNING,
      attempts: attempt,
      inputs: JSON.stringify(inputs),
      error: null,
      skip_reason: null,
      started_at: now,
      updated_at: now,
    });
    this.store.appendEvent(rt.runId, graphNode.id, 'node_started', `开始执行（第 ${attempt} 次尝试）`, {
      attempt,
      deps: graphNode.dependsOn,
    });
    const controller = new AbortController();
    rt.active.set(graphNode.id, controller);

    const task = this.#execute(rt, graphNode, inputs, attempt, controller)
      .catch((err) => {
        if (!this.closed) this.logger.error?.(`节点执行异常 ${graphNode.id}:`, err);
      })
      .finally(() => {
        this.tasks.delete(task);
        rt.active.delete(graphNode.id);
        if (!this.closed) {
          this.#schedule(rt);
          this.#maybeFinalize(rt);
        }
      });
    this.tasks.add(task);
  }

  async #execute(rt, graphNode, inputs, attempt, controller) {
    if (!this.closed) {
      this.store.updateNodeRun(rt.runId, graphNode.id, {
        attempts: attempt,
        updated_at: Date.now(),
      });
    }
    const ctx = this.#makeContext(rt, graphNode, attempt, controller.signal);
    ctx.inputs = inputs;
    ctx.inputDigest = sha256(stableJson(inputs));
    try {
      const executor = this.executors.get(graphNode.type);
      if (!executor) throw new Error(`没有注册节点类型 ${graphNode.type} 的执行器`);
      const result = (await executor(ctx)) || {};
      const current = this.store.getNodeRun(rt.runId, graphNode.id);
      if (!current || current.status !== NODE_STATUS.RUNNING) return; // cancelled meanwhile
      const finishedAt = Date.now();
      this.store.updateNodeRun(rt.runId, graphNode.id, {
        status: NODE_STATUS.SUCCEEDED,
        outputs: JSON.stringify(result.output ?? {}),
        meta: JSON.stringify({ ...(result.meta ?? {}), inputDigest: ctx.inputDigest }),
        finished_at: finishedAt,
        updated_at: finishedAt,
      });
      this.store.appendEvent(
        rt.runId,
        graphNode.id,
        'node_succeeded',
        `执行成功（第 ${attempt} 次尝试，耗时 ${finishedAt - (current.started_at ?? finishedAt)} ms）`,
        { output: result.output ?? {} },
      );
      if (graphNode.pauseAfter) {
        this.store.updateRun(rt.runId, {
          status: 'paused',
          gate: `pause_after:${graphNode.id}`,
          updated_at: Date.now(),
        });
        rt.paused = true;
        this.store.appendEvent(
          rt.runId,
          graphNode.id,
          'paused',
          `人工暂停点：节点 ${graphNode.label} 已完成，等待人工恢复`,
          { gate: `pause_after:${graphNode.id}` },
        );
      }
    } catch (err) {
      const current = this.store.getNodeRun(rt.runId, graphNode.id);
      if (!current || current.status !== NODE_STATUS.RUNNING) return;
      if (isAbortError(err) && this.closed && !rt.cancelled) {
        // Graceful shutdown: leave the node as `running` so the next process
        // requeues it (recover()) instead of treating it as a user cancel.
        this.store.appendEvent(
          rt.runId,
          graphNode.id,
          'node_interrupted',
          '实例关闭时中断执行，重启后重新排队',
        );
        return;
      }
      if (isAbortError(err) || rt.cancelled) {
        this.store.updateNodeRun(rt.runId, graphNode.id, {
          status: NODE_STATUS.CANCELLED,
          error: '运行已取消',
          finished_at: Date.now(),
          updated_at: Date.now(),
        });
        this.store.appendEvent(rt.runId, graphNode.id, 'node_cancelled', '执行被取消');
        return;
      }
      const message = err?.message || String(err);
      const retryable = err?.retryable !== false;
      const maxAttempts = graphNode.retry?.maxAttempts ?? 1;
      if (retryable && attempt < maxAttempts) {
        const delay = this.#backoff(graphNode, attempt);
        this.store.updateNodeRun(rt.runId, graphNode.id, {
          error: `第 ${attempt} 次尝试失败：${message}`,
          updated_at: Date.now(),
        });
        this.store.appendEvent(
          rt.runId,
          graphNode.id,
          'node_retrying',
          `第 ${attempt}/${maxAttempts} 次尝试失败，${delay} ms 后重试：${message}`,
        );
        if (delay > 0) await abortableSleep(delay, controller.signal).catch(() => {});
        const refreshed = this.store.getNodeRun(rt.runId, graphNode.id);
        if (!refreshed || refreshed.status !== NODE_STATUS.RUNNING || rt.cancelled || this.closed) return;
        await this.#execute(rt, graphNode, inputs, attempt + 1, controller);
        return;
      }
      this.store.updateNodeRun(rt.runId, graphNode.id, {
        status: NODE_STATUS.FAILED,
        error: message,
        finished_at: Date.now(),
        updated_at: Date.now(),
      });
      this.store.appendEvent(
        rt.runId,
        graphNode.id,
        'node_failed',
        `执行失败（已尝试 ${attempt}/${maxAttempts} 次）：${message}`,
      );
    }
  }

  #backoff(graphNode, attempt) {
    const { backoffMs = 0, backoffFactor = 1, maxBackoffMs = 30_000 } = graphNode.retry ?? {};
    return Math.min(maxBackoffMs, Math.round(backoffMs * backoffFactor ** (attempt - 1)));
  }

  #makeContext(rt, graphNode, attempt, signal) {
    const { runId } = rt;
    const logLevels = new Set(['debug', 'info', 'warn', 'error']);
    return {
      runId,
      nodeId: graphNode.id,
      node: graphNode,
      attempt,
      signal,
      config: graphNode.config ?? {},
      runsDir: this.runsDir,
      runDir: `${this.runsDir}/${runId}`,
      inputs: {},
      inputDigest: null,
      log: (level, message, detail) => {
        const lvl = logLevels.has(level) ? level : 'info';
        const entry = { at: Date.now(), level: lvl, message: String(message), attempt };
        this.store.appendNodeLog(runId, graphNode.id, entry);
        this.store.appendEvent(runId, graphNode.id, 'log', entry.message, detail ?? null);
      },
      sleep: (ms) => abortableSleep(ms, signal),
      /**
       * Crash-safe external effect. The ledger is checked and written by the
       * engine; the executor never repeats a confirmed key.
       */
      effect: async (key, meta, fn) => {
        if (this.store.hasEffect(runId, graphNode.id, key)) {
          this.store.appendNodeLog(runId, graphNode.id, {
            at: Date.now(),
            level: 'info',
            message: `副作用 ${key} 已确认，跳过重复执行并复用结果`,
            attempt,
          });
          this.store.appendEvent(runId, graphNode.id, 'effect_adopted', `副作用已确认，跳过重复执行：${key}`, meta);
          return { adopted: true, key };
        }
        const value = await fn();
        this.store.commitEffect(runId, graphNode.id, key, meta ?? {});
        this.store.appendNodeLog(runId, graphNode.id, {
          at: Date.now(),
          level: 'info',
          message: `副作用 ${key} 已提交`,
          attempt,
        });
        this.store.appendEvent(runId, graphNode.id, 'effect_committed', `副作用已提交：${key}`, meta);
        return { adopted: false, key, value };
      },
    };
  }

  #snapshots(runId) {
    const map = new Map();
    for (const row of this.store.listNodeRuns(runId)) {
      map.set(row.node_id, {
        status: row.status,
        output: parseJson(row.outputs, {}),
        input: parseJson(row.inputs, {}),
        meta: parseJson(row.meta, {}),
      });
    }
    return map;
  }

  #blockNode(rt, graphNode, badDep, reason) {
    const now = Date.now();
    this.store.updateNodeRun(rt.runId, graphNode.id, {
      status: NODE_STATUS.BLOCKED,
      error: reason ?? `上游节点 ${badDep.node_id} 状态为 ${badDep.status}，输入不完整`,
      finished_at: now,
      updated_at: now,
    });
    this.store.appendEvent(
      rt.runId,
      graphNode.id,
      'node_blocked',
      `阻塞：上游 ${badDep.node_id} 未成功（${badDep.status}），拒绝消费不完整输入`,
    );
  }

  #skipNode(rt, graphNode, reasonCode, reason) {
    const now = Date.now();
    this.store.updateNodeRun(rt.runId, graphNode.id, {
      status: NODE_STATUS.SKIPPED,
      skip_reason: reasonCode,
      error: reason,
      finished_at: now,
      updated_at: now,
    });
    this.store.appendEvent(rt.runId, graphNode.id, 'node_skipped', `跳过：${reason}`);
  }

  #failNode(rt, graphNode, message) {
    const now = Date.now();
    this.store.updateNodeRun(rt.runId, graphNode.id, {
      status: NODE_STATUS.FAILED,
      error: message,
      finished_at: now,
      updated_at: now,
    });
    this.store.appendEvent(rt.runId, graphNode.id, 'node_failed', message);
  }

  #maybeFinalize(rt) {
    if (rt.finishing) return;
    const run = this.store.getRun(rt.runId);
    if (!run || RUN_TERMINAL.has(run.status) || run.status === 'paused') return;
    const rows = this.store.listNodeRuns(rt.runId);
    if (rows.some((r) => !isTerminal(r.status))) return;
    rt.finishing = true;
    const failed = rows.some((r) => [NODE_STATUS.FAILED, NODE_STATUS.BLOCKED].includes(r.status));
    const status = failed ? 'failed' : 'succeeded';
    this.store.updateRun(rt.runId, { status, gate: null, finished_at: Date.now(), updated_at: Date.now() });
    const counts = countBy(rows, 'status');
    this.store.appendEvent(
      rt.runId,
      null,
      status === 'succeeded' ? 'run_succeeded' : 'run_failed',
      status === 'succeeded'
        ? `运行完成：${counts.succeeded ?? 0} 个节点成功，${counts.skipped ?? 0} 个跳过`
        : `运行结束：${counts.failed ?? 0} 个失败，${counts.blocked ?? 0} 个阻塞`,
      { counts },
    );
    this.#finishRuntime(rt.runId);
  }

  #finishRuntime(runId) {
    const rt = this.runtimes.get(runId);
    if (!rt) return;
    rt.finishing = true;
    this.runtimes.delete(runId);
  }

  #presentNode(runId, row, graph, effects) {
    const graphNode = graph.nodes.find((n) => n.id === row.node_id) ?? {};
    const nodeEffects = effects.filter((e) => e.node_id === row.node_id);
    return {
      id: row.node_id,
      label: graphNode.label ?? row.node_id,
      type: graphNode.type ?? 'custom',
      dependsOn: graphNode.dependsOn ?? [],
      skipWhen: graphNode.skipWhen ?? null,
      retry: graphNode.retry ?? {},
      status: row.status,
      attempt: row.attempts,
      output: parseJson(row.outputs, null),
      meta: parseJson(row.meta, null),
      input: parseJson(row.inputs, null),
      error: row.error,
      skipReason: row.skip_reason,
      startedAt: row.started_at,
      finishedAt: row.finished_at,
      durationMs:
        row.started_at != null
          ? Math.max(0, (row.finished_at ?? Date.now()) - row.started_at)
          : null,
      logs: parseJson(row.logs, []),
      effects: nodeEffects,
    };
  }

  /** Resolves when the run reaches one of the statuses (poll-free via events). */
  waitForStatus(runId, statuses, { timeoutMs = 15_000 } = {}) {
    const wanted = new Set(Array.isArray(statuses) ? statuses : [statuses]);
    return new Promise((resolve, reject) => {
      const started = Date.now();
      const check = () => {
        const run = this.store.getRun(runId);
        if (!run) {
          reject(new Error(`运行 ${runId} 不存在`));
          return;
        }
        if (wanted.has(run.status)) {
          resolve(run);
          return;
        }
        if (Date.now() - started > timeoutMs) {
          reject(new Error(`等待运行状态超时（当前 ${run.status}）`));
          return;
        }
        setTimeout(check, 15);
      };
      check();
    });
  }

  /** Waits until every node has a terminal status (even if the run stays paused). */
  waitForIdle(runId, { timeoutMs = 15_000 } = {}) {
    return new Promise((resolve, reject) => {
      const started = Date.now();
      const check = () => {
        const rows = this.store.listNodeRuns(runId);
        if (rows.every((r) => isTerminal(r.status))) {
          resolve(rows);
          return;
        }
        if (Date.now() - started > timeoutMs) {
          reject(new Error('等待节点收敛超时'));
          return;
        }
        setTimeout(check, 15);
      };
      check();
    });
  }

  close() {
    this.closed = true;
    for (const rt of this.runtimes.values()) {
      for (const controller of rt.active.values()) controller.abort();
    }
    this.runtimes.clear();
  }

  /**
   * Graceful shutdown: stop scheduling, abort in-flight nodes, and wait for
   * their cancellation to be persisted. Without this wait a quick restart can
   * read a half-written handover (or worse, the old process writes afterwards).
   */
  async shutdown({ timeoutMs = 3000 } = {}) {
    this.close();
    if (!this.tasks.size) return;
    await Promise.race([
      Promise.allSettled([...this.tasks]),
      new Promise((resolve) => setTimeout(resolve, timeoutMs).unref?.()),
    ]);
  }
}

export class EngineError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'EngineError';
    this.code = code;
  }
}

function countBy(rows, key) {
  const out = {};
  for (const row of rows) out[row[key]] = (out[row[key]] ?? 0) + 1;
  return out;
}

function abortableSleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(abortError());
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    function onAbort() {
      clearTimeout(timer);
      reject(abortError());
    }
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
