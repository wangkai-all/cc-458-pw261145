import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { Engine, EngineError } from './engine.js';
import { createExecutors } from './executors.js';
import { compileGraph, GraphError } from './graph.js';
import { openStore } from './store.js';
import { FileTrigger } from './trigger.js';
import { readJson } from './util.js';

/**
 * Builds the HTTP application with an isolated data directory. The CLI entry
 * point (server.js) and the integration tests both use this, so the tested
 * surface is the shipped surface.
 */
export function createApp({
  rootDir,
  dataDir,
  sourceDir,
  publicDir = path.join(rootDir, 'public'),
  concurrency = 4,
  logger = console,
  triggerFile = path.join(dataDir, 'trigger.json'),
  triggerPollMs = 400,
  recoverOnBoot = true,
}) {
  fs.mkdirSync(dataDir, { recursive: true });
  const store = openStore(path.join(dataDir, 'orchestrator.db'));
  const engine = new Engine({
    store,
    runsDir: path.join(dataDir, 'runs'),
    concurrency,
    logger,
  }).registerAll(createExecutors({ sourceDir, outboxDir: path.join(dataDir, 'outbox') }));

  const recovered = recoverOnBoot ? engine.recover() : [];
  const trigger = new FileTrigger({ filePath: triggerFile, store, engine, logger }).start(triggerPollMs);

  const server = http.createServer((req, res) => {
    handle(req, res).catch((err) => sendError(res, err));
  });

  async function handle(req, res) {
    const url = new URL(req.url, `http://${req.headers.host ?? 'localhost'}`);
    if (url.pathname.startsWith('/api/')) {
      await handleApi(req, res, url);
      return;
    }
    serveStatic(res, url.pathname);
  }

  async function handleApi(req, res, url) {
    const parts = url.pathname.split('/').filter(Boolean);
    const method = req.method ?? 'GET';

    if (parts[1] === 'health') {
      return sendJson(res, 200, {
        ok: true,
        concurrency,
        recovered,
        dataDir: path.relative(rootDir, dataDir) || dataDir,
      });
    }

    if (parts[1] === 'pipelines' && parts.length === 2 && method === 'GET') {
      return sendJson(
        res,
        200,
        store.listPipelines().map((p) => ({
          id: p.id,
          name: p.name,
          graphHash: p.graph_hash,
          updatedAt: p.updated_at,
          nodeCount: JSON.parse(p.definition).nodes.length,
        })),
      );
    }

    if (parts[1] === 'pipelines' && parts.length === 3 && method === 'GET') {
      const row = store.getPipeline(parts[2]);
      if (!row) return sendJson(res, 404, { error: '流水线不存在' });
      return sendJson(res, 200, JSON.parse(row.definition));
    }

    if (parts[1] === 'fixtures' && method === 'GET') {
      return sendJson(res, 200, listFixtures());
    }

    if (parts[1] === 'fixtures' && parts.length === 3 && method === 'GET') {
      const file = safeFixturePath(parts[2]);
      if (!file) return sendJson(res, 404, { error: '夹具不存在' });
      return sendJson(res, 200, readJson(file));
    }

    if (parts[1] === 'validate' && method === 'POST') {
      const body = (await readBody(req)) ?? {};
      const raw = body.definition ?? (body.fixture ? readFixture(body.fixture) : null);
      if (!raw) return sendJson(res, 400, { error: '缺少 definition 或 fixture' });
      try {
        const graph = compileGraph(raw);
        return sendJson(res, 200, {
          ok: true,
          graphHash: graph.hash,
          order: graph.order,
          nodes: graph.nodes.length,
        });
      } catch (err) {
        if (err instanceof GraphError) {
          return sendJson(res, 422, {
            ok: false,
            code: err.code,
            message: err.message,
            details: err.details,
          });
        }
        throw err;
      }
    }

    if (parts[1] === 'runs' && parts.length === 2) {
      if (method === 'GET') return sendJson(res, 200, store.listRuns(80));
      if (method === 'POST') {
        const body = (await readBody(req)) ?? {};
        let raw = body.definition ?? null;
        if (!raw && body.fixture) {
          raw = readFixture(body.fixture);
          if (!raw) return sendJson(res, 404, { error: '夹具不存在' });
        }
        if (!raw) raw = readPipelineDefinition(body.pipelineId);
        if (!raw) return sendJson(res, 400, { error: '缺少 definition、fixture 或 pipelineId' });
        let graph;
        try {
          graph = compileGraph(raw);
        } catch (err) {
          if (err instanceof GraphError) {
            return sendJson(res, 422, {
              ok: false,
              code: err.code,
              message: err.message,
              details: err.details,
            });
          }
          throw err;
        }
        const run = engine.createRun(graph);
        engine.start(run.id);
        return sendJson(res, 201, { runId: run.id, graphHash: graph.hash, order: graph.order });
      }
    }

    if (parts[1] === 'trigger' && method === 'POST') {
      const body = (await readBody(req)) ?? {};
      if (!body.fixture) return sendJson(res, 400, { error: '缺少 fixture' });
      if (!safeFixturePath(body.fixture)) return sendJson(res, 404, { error: '夹具不存在' });
      const spec = {
        graph: safeFixturePath(body.fixture),
        at: body.at ?? new Date().toISOString(),
        inputs: body.inputs ?? {},
        overrides: body.overrides ?? {},
      };
      fs.mkdirSync(path.dirname(triggerFile), { recursive: true });
      fs.writeFileSync(triggerFile, `${JSON.stringify(spec, null, 2)}\n`, 'utf8');
      const dispatched = await trigger.poll();
      return sendJson(res, 202, { spec, dispatched });
    }

    if (parts[1] === 'runs' && parts.length >= 3) {
      const runId = parts[2];
      if (!store.getRun(runId)) return sendJson(res, 404, { error: '运行不存在' });

      if (parts.length === 3 && method === 'GET') return sendJson(res, 200, engine.describeRun(runId));
      if (parts.length === 4 && parts[3] === 'events' && method === 'GET') {
        const after = Number(url.searchParams.get('after') ?? 0);
        return sendJson(res, 200, {
          events: store.listEvents(runId, Number.isFinite(after) ? after : 0, 1000),
        });
      }
      if (parts.length === 4 && parts[3] === 'stream' && method === 'GET') {
        return streamRun(req, res, runId);
      }
      if (parts.length === 4 && parts[3] === 'retry-scope' && method === 'GET') {
        const nodeId = url.searchParams.get('nodeId');
        if (!nodeId) return sendJson(res, 400, { error: '缺少 nodeId' });
        return sendJson(res, 200, { scope: engine.retryScope(runId, nodeId) });
      }
      if (parts.length === 4 && parts[3] === 'pause' && method === 'POST') {
        return sendJson(res, 200, { run: engine.pause(runId) });
      }
      if (parts.length === 4 && parts[3] === 'resume' && method === 'POST') {
        return sendJson(res, 200, { run: engine.resume(runId) });
      }
      if (parts.length === 4 && parts[3] === 'cancel' && method === 'POST') {
        return sendJson(res, 200, { run: engine.cancel(runId) });
      }
      if (parts.length === 4 && parts[3] === 'retry' && method === 'POST') {
        const body = (await readBody(req)) ?? {};
        if (!body.nodeId) return sendJson(res, 400, { error: '缺少 nodeId' });
        const result = engine.retry(runId, body.nodeId);
        return sendJson(res, 202, { run: result.run, scope: result.scope });
      }
    }

    sendJson(res, 404, { error: `未知接口 ${req.method} ${url.pathname}` });
  }

  function streamRun(req, res, runId) {
    res.writeHead(200, {
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    });
    res.write('retry: 2000\n\n');

    let lastSeq = 0;
    let lastUpdatedAt = 0;
    let closed = false;
    const send = (event, data) => {
      if (closed) return;
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    };
    const initial = engine.describeRun(runId);
    if (!initial) {
      send('deleted', {});
      res.end();
      return;
    }
    lastSeq = initial.events.at(-1)?.seq ?? 0;
    lastUpdatedAt = initial.run.updated_at;
    send('snapshot', initial);

    const timer = setInterval(() => {
      if (closed) return;
      const run = store.getRun(runId);
      if (!run) {
        send('deleted', {});
        cleanup();
        res.end();
        return;
      }
      const events = store.listEvents(runId, lastSeq, 200);
      if (events.length) {
        lastSeq = events.at(-1).seq;
        send('events', { events, runStatus: run.status, gate: run.gate, updatedAt: run.updated_at });
      } else if (run.updated_at !== lastUpdatedAt) {
        send('heartbeat', { runStatus: run.status, gate: run.gate, updatedAt: run.updated_at });
      }
      lastUpdatedAt = run.updated_at;
      if (['succeeded', 'failed', 'cancelled'].includes(run.status) && !events.length) {
        send('snapshot', engine.describeRun(runId));
        send('closed', { runStatus: run.status });
        cleanup();
        res.end();
      }
    }, 250);
    timer.unref?.();

    function cleanup() {
      if (closed) return;
      closed = true;
      clearInterval(timer);
    }
    req.on('close', cleanup);
    res.on('close', cleanup);
    res.on('error', cleanup);
  }

  function readPipelineDefinition(pipelineId) {
    if (!pipelineId) return null;
    const row = store.getPipeline(pipelineId);
    return row ? JSON.parse(row.definition) : null;
  }

  function listFixtures() {
    const dir = path.join(rootDir, 'fixtures', 'graphs');
    if (!fs.existsSync(dir)) return [];
    return fs
      .readdirSync(dir)
      .filter((f) => f.endsWith('.json'))
      .map((file) => {
        const raw = readJson(path.join(dir, file));
        let validation = { ok: true };
        try {
          const graph = compileGraph(raw);
          validation = { ok: true, graphHash: graph.hash, order: graph.order };
        } catch (err) {
          validation = {
            ok: false,
            code: err.code ?? 'INVALID_GRAPH',
            message: err.message,
            details: err.details ?? {},
          };
        }
        return {
          file,
          name: raw.name ?? file,
          description: raw.description ?? '',
          nodeCount: raw.nodes?.length ?? 0,
          validation,
        };
      });
  }

  function safeFixturePath(file) {
    if (typeof file !== 'string' || file.includes('/') || file.includes('\\') || file.includes('..')) {
      return null;
    }
    const p = path.join(rootDir, 'fixtures', 'graphs', file);
    return fs.existsSync(p) ? p : null;
  }

  function readFixture(file) {
    const p = safeFixturePath(file);
    return p ? readJson(p) : null;
  }

  function serveStatic(res, pathname) {
    const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
    const filePath = path.normalize(path.join(publicDir, rel));
    if (!filePath.startsWith(publicDir)) {
      res.writeHead(403).end('forbidden');
      return;
    }
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }).end('not found');
        return;
      }
      res.writeHead(200, {
        'content-type': MIME[path.extname(filePath)] ?? 'application/octet-stream',
        'cache-control': 'no-cache',
      });
      res.end(data);
    });
  }

  function sendJson(res, status, payload) {
    const body = JSON.stringify(payload);
    res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(body);
  }

  function sendError(res, err) {
    const status =
      err instanceof GraphError
        ? 422
        : err instanceof EngineError
          ? err.code === 'NOT_FOUND'
            ? 404
            : 409
          : 500;
    if (status === 500) logger.error?.(err);
    if (res.headersSent) {
      res.end();
      return;
    }
    sendJson(res, status, { error: err.message, code: err.code ?? 'INTERNAL' });
  }

  function close() {
    trigger.stop();
    let done = false;
    let timer = null;
    return new Promise((resolve) => {
      const finish = async () => {
        if (done) return;
        done = true;
        if (timer) clearTimeout(timer);
        await engine.shutdown({ timeoutMs: 1000 });
        try {
          store.close();
        } catch {}
        resolve();
      };
      // Stop accepting work first, then let in-flight nodes persist their
      // cancellation, then release SQLite. A restart racing this gets either a
      // running node (requeued) or a cancelled node — never a lost write.
      server.closeAllConnections?.();
      server.close(() => {
        finish().catch(() => {});
      });
      timer = setTimeout(() => {
        finish().catch(() => {});
      }, 1500);
      timer.unref?.();
    });
  }

  return { server, store, engine, trigger, recovered, close };
}

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

function readBody(req) {
  return new Promise((resolve, reject) => {
    if (!['POST', 'PUT', 'PATCH'].includes(req.method ?? '')) {
      resolve(null);
      return;
    }
    let size = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > 2 * 1024 * 1024) {
        reject(new Error('请求体过大'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (!chunks.length) {
        resolve(null);
        return;
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch {
        reject(new Error('请求体不是合法 JSON'));
      }
    });
    req.on('error', reject);
  });
}
