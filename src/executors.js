import fs from 'node:fs';
import path from 'node:path';
import { atomicWriteJson, readJson, sha256, stableJson } from './util.js';

/**
 * Executors are intentionally small and fixture-backed: the point of the
 * system is durable orchestration, not crawling.
 *
 * Every executor receives ctx (see Engine#makeContext) and returns
 * { output, meta }. Output fields are what downstream conditions can read.
 */
export function createExecutors({ sourceDir, outboxDir, logger = console }) {
  const readSource = (name) => {
    // Resolve and verify the target stays inside the configured source dir:
    // source names are user-supplied fixture keys, never paths.
    const base = path.resolve(sourceDir);
    const candidates = [`${name}.json`, name];
    for (const rel of candidates) {
      const p = path.resolve(base, rel);
      if (!p.startsWith(`${base}${path.sep}`)) continue;
      if (fs.existsSync(p)) return readJson(p);
    }
    throw new Error(`数据源不存在：${name}（目录 ${sourceDir}）`);
  };

  const maybeFail = (ctx, stage) => {
    const failFirst = Number(ctx.config.failFirstAttempts ?? 0);
    if (ctx.attempt <= failFirst) {
      const err = new Error(`${stage} 模拟失败（第 ${ctx.attempt} 次尝试）`);
      err.retryable = true;
      throw err;
    }
    const failHard = ctx.config.failHard === true;
    if (failHard) {
      const err = new Error(`${stage} 模拟不可重试失败`);
      err.retryable = false;
      throw err;
    }
  };

  const download = async (ctx) => {
    const source = String(ctx.config.source ?? 'orders');
    ctx.log('info', `开始下载数据源 ${source}`);
    await ctx.sleep(Number(ctx.config.delayMs ?? 40));
    const payload = readSource(source);
    const rows = Array.isArray(payload) ? payload : (payload.rows ?? []);
    maybeFail(ctx, '下载');
    const artifact = path.join(ctx.runDir, `${ctx.nodeId}.json`);
    atomicWriteJson(artifact, { source, rows, fetchedAt: Date.now() });
    const digest = sha256(stableJson(rows));
    ctx.log('info', `下载完成：${rows.length} 行，摘要 ${digest.slice(0, 12)}`);
    return {
      output: { artifact: `${ctx.nodeId}.json`, rows: rows.length, digest },
      meta: { source, bytes: Buffer.byteLength(JSON.stringify(rows)) },
    };
  };

  const parse = async (ctx) => {
    const upstream = Object.values(ctx.inputs)[0];
    if (!upstream?.artifact) {
      const err = new Error('缺少上游下载产物');
      err.retryable = false;
      throw err;
    }
    await ctx.sleep(Number(ctx.config.delayMs ?? 30));
    const file = path.join(ctx.runDir, upstream.artifact);
    const raw = readJson(file);
    maybeFail(ctx, '解析');
    const sourceRows = ctx.config.emptyRows === true ? [] : raw.rows;
    const rows = sourceRows.map((row, index) => ({
      index,
      id: String(row.id ?? `row-${index}`),
      amount: Number(row.amount ?? 0),
      currency: String(row.currency ?? 'CNY'),
      email: row.email == null ? null : String(row.email),
      tags: Array.isArray(row.tags) ? row.tags : [],
    }));
    const artifact = path.join(ctx.runDir, `${ctx.nodeId}.json`);
    atomicWriteJson(artifact, { rows });
    const digest = sha256(stableJson(rows));
    ctx.log('info', `解析完成：${rows.length} 条记录，摘要 ${digest.slice(0, 12)}`);
    return {
      output: { artifact: `${ctx.nodeId}.json`, count: rows.length, digest },
      meta: { dialect: 'json', columns: ['id', 'amount', 'currency', 'email', 'tags'] },
    };
  };

  const validate = async (ctx) => {
    const upstream = Object.values(ctx.inputs)[0];
    if (!upstream?.artifact) {
      const err = new Error('缺少上游解析产物');
      err.retryable = false;
      throw err;
    }
    await ctx.sleep(Number(ctx.config.delayMs ?? 25));
    const parsed = readJson(path.join(ctx.runDir, upstream.artifact));
    const minRows = Number(ctx.config.minRows ?? 0);
    const requireEmail = ctx.config.requireEmail !== false;
    const rejected = [];
    const accepted = [];
    for (const row of parsed.rows) {
      if (!row.id) rejected.push({ id: row.id, reason: 'id 缺失' });
      else if (!Number.isFinite(row.amount)) rejected.push({ id: row.id, reason: 'amount 非法' });
      else if (requireEmail && !row.email) rejected.push({ id: row.id, reason: 'email 缺失' });
      else accepted.push(row);
    }
    maybeFail(ctx, '校验');
    if (accepted.length < minRows) {
      const err = new Error(`有效记录 ${accepted.length} 条，低于门限 ${minRows} 条`);
      err.retryable = false;
      throw err;
    }
    const artifact = path.join(ctx.runDir, `${ctx.nodeId}.json`);
    atomicWriteJson(artifact, { accepted, rejected });
    const digest = sha256(stableJson(accepted));
    ctx.log(
      rejected.length ? 'warn' : 'info',
      `校验完成：通过 ${accepted.length} 条，拒绝 ${rejected.length} 条，摘要 ${digest.slice(0, 12)}`,
    );
    return {
      output: { artifact: `${ctx.nodeId}.json`, accepted: accepted.length, rejected: rejected.length, digest },
      meta: { minRows, rejectedSample: rejected.slice(0, 3) },
    };
  };

  /**
   * Publish is the only node with an irreversible external write. It goes
   * through ctx.effect so a confirmed publish is adopted, never repeated — the
   * key is derived from the exact validated input digest.
   */
  const publish = async (ctx) => {
    const upstream = Object.values(ctx.inputs)[0];
    if (!upstream?.digest) {
      const err = new Error('缺少上游校验摘要');
      err.retryable = false;
      throw err;
    }
    const target = String(ctx.config.target ?? ctx.nodeId);
    const key = `publish:${target}:${upstream.digest.slice(0, 16)}`;
    const file = path.join(outboxDir, `${target}.ndjson`);
    await ctx.sleep(Number(ctx.config.delayMs ?? 30));

    let value = null;
    const result = await ctx.effect(key, { target, digest: upstream.digest, key }, async () => {
      const record = {
        key,
        target,
        digest: upstream.digest,
        accepted: upstream.accepted ?? null,
        at: Date.now(),
      };
      fs.mkdirSync(outboxDir, { recursive: true });
      fs.appendFileSync(file, `${JSON.stringify(record)}\n`, 'utf8');
      atomicWriteJson(path.join(ctx.runDir, `${ctx.nodeId}.receipt.json`), record);
      maybeFail(ctx, '发布');
      if (ctx.config.crashAfterEffect === true) {
        ctx.log('warn', '模拟：副作用已提交、节点状态未确认时进程中断');
        setTimeout(() => process.kill(process.pid, 'SIGKILL'), 15);
        await ctx.sleep(10_000);
      }
      return record;
    });
    value = result.value ?? { key, target, adopted: true };
    ctx.log(
      result.adopted ? 'info' : 'info',
      result.adopted ? `发布 ${target} 已存在，复用已确认副作用` : `发布完成：${target}`,
    );
    return {
      output: { target, key, adopted: result.adopted, file: `${target}.ndjson` },
      meta: { publishKey: key, adopted: result.adopted },
    };
  };

  const custom = async (ctx) => {
    const label = String(ctx.config.label ?? ctx.nodeId);
    ctx.log('info', `执行自定义步骤 ${label}`);
    await ctx.sleep(Number(ctx.config.delayMs ?? 20));
    if (ctx.config.failIfFile && fs.existsSync(ctx.config.failIfFile)) {
      const err = new Error(`${label} 因标记文件存在而失败`);
      err.retryable = true;
      throw err;
    }
    maybeFail(ctx, '自定义步骤');
    const data = ctx.config.emit ?? { ok: true };
    ctx.log('info', `自定义步骤 ${label} 完成`);
    return { output: data, meta: { label } };
  };

  return { download, parse, validate, publish, custom };
}
