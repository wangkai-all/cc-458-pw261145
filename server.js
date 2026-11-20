import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createApp } from './src/app.js';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 4173);
const DATA_DIR = path.resolve(process.env.DATA_DIR ?? path.join(ROOT, 'data'));
const SOURCE_DIR = path.resolve(process.env.SOURCE_DIR ?? path.join(ROOT, 'fixtures', 'source'));
const CONCURRENCY = clampInt(process.env.CONCURRENCY, 4, 1, 16);

const app = createApp({
  rootDir: ROOT,
  dataDir: DATA_DIR,
  sourceDir: SOURCE_DIR,
  concurrency: CONCURRENCY,
});

if (app.recovered.length) {
  console.log(`[recover] 已恢复 ${app.recovered.length} 个运行：`, app.recovered);
}

app.server.listen(PORT, () => {
  console.log(
    `采集编排器已启动：http://localhost:${PORT}\n  data=${DATA_DIR}\n  source=${SOURCE_DIR}\n  concurrency=${CONCURRENCY}`,
  );
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    console.log(`\n收到 ${signal}，停止调度并落盘…`);
    app.close().then(() => process.exit(0));
    setTimeout(() => process.exit(0), 2000).unref();
  });
}

function clampInt(value, fallback, min, max) {
  const n = Number.parseInt(value ?? '', 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}
