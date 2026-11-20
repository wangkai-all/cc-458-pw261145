import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

export function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

/** Deterministic JSON so hashes do not depend on object key insertion order. */
export function stableJson(value) {
  return JSON.stringify(sortValue(value));
}

function sortValue(value) {
  if (Array.isArray(value)) return value.map(sortValue);
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) out[key] = sortValue(value[key]);
    return out;
  }
  return value;
}

export function atomicWriteJson(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.tmp-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
  fs.writeFileSync(tmp, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
  fs.renameSync(tmp, filePath);
}

export function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

export function parseJson(text, fallback = null) {
  if (text == null || text === '') return fallback;
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

export function abortError(message = '操作已取消') {
  const err = new Error(message);
  err.name = 'AbortError';
  return err;
}

export function isAbortError(err) {
  return err?.name === 'AbortError';
}
