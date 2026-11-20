import { sha256, stableJson } from './util.js';

export class GraphError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'GraphError';
    this.code = code;
    this.details = details;
  }
}

const NODE_TYPES = new Set(['download', 'parse', 'validate', 'publish', 'custom']);
const ID_RE = /^[A-Za-z][A-Za-z0-9_-]{0,63}$/;
const TOKEN_RE =
  /\s*(?:(?<op>&&|\|\||==|!=|!|\(|\)|\.)|(?<str>'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")|(?<num>-?\d+(?:\.\d+)?)|(?<id>[A-Za-z][A-Za-z0-9_-]*))/y;

/**
 * Normalizes and validates a pipeline definition.
 *
 * {
 *   id?: string, name: string, description?: string,
 *   nodes: [{
 *     id, type: download|parse|validate|publish|custom, label?,
 *     dependsOn?: string[], skipWhen?: string,
 *     retry?: { maxAttempts?, backoffMs?, backoffFactor?, maxBackoffMs? },
 *     pauseAfter?: boolean, config?: object
 *   }]
 * }
 *
 * Rules: acyclic; every dependency exists; conditions may only reference
 * ancestors (statically checked), so a node can never read an input that is
 * not upstream of it.
 */
export function compileGraph(raw) {
  if (!raw || typeof raw !== 'object') throw new GraphError('INVALID_GRAPH', '流水线定义必须是对象');
  if (!Array.isArray(raw.nodes) || raw.nodes.length === 0) {
    throw new GraphError('INVALID_GRAPH', 'nodes 必须是非空数组');
  }
  const seen = new Set();
  const nodes = raw.nodes.map((n, index) => compileNode(n, index, seen));
  const byId = new Map(nodes.map((n) => [n.id, n]));

  for (const node of nodes) {
    for (const dep of node.dependsOn) {
      if (!byId.has(dep)) {
        throw new GraphError('UNKNOWN_DEPENDENCY', `节点 ${node.id} 依赖了不存在的节点 ${dep}`, {
          nodeId: node.id,
          dependency: dep,
        });
      }
    }
  }

  const order = topologicalOrder(nodes, byId);
  const ancestors = new Map();
  for (const id of order) {
    const acc = new Set();
    for (const dep of byId.get(id).dependsOn) {
      acc.add(dep);
      for (const a of ancestors.get(dep)) acc.add(a);
    }
    ancestors.set(id, acc);
  }

  for (const node of nodes) {
    if (!node.skipWhen) continue;
    for (const ref of conditionReferences(node.skipWhen)) {
      if (!byId.has(ref.nodeId)) {
        throw new GraphError('INVALID_CONDITION', `节点 ${node.id} 的跳过条件引用了未知节点 ${ref.nodeId}`, {
          nodeId: node.id,
        });
      }
      if (!ancestors.get(node.id).has(ref.nodeId)) {
        throw new GraphError(
          'INVALID_CONDITION',
          `节点 ${node.id} 的跳过条件引用了非上游节点 ${ref.nodeId}；条件只能读取已完成的输入`,
          { nodeId: node.id, reference: ref.nodeId },
        );
      }
    }
  }

  const graph = {
    id: raw.id || 'graph',
    name: raw.name || '未命名流水线',
    description: raw.description || '',
    version: typeof raw.version === 'number' ? raw.version : 1,
    nodes,
    order,
  };
  graph.hash = sha256(stableJson(stripPresentation(graph)));
  return graph;
}

function stripPresentation(graph) {
  return {
    id: graph.id,
    name: graph.name,
    nodes: graph.nodes.map((n) => ({
      id: n.id,
      type: n.type,
      label: n.label,
      dependsOn: n.dependsOn,
      skipWhen: n.skipWhen,
      retry: n.retry,
      pauseAfter: n.pauseAfter,
      config: n.config,
    })),
  };
}

function compileNode(raw, index, seen) {
  if (!raw || typeof raw !== 'object') {
    throw new GraphError('INVALID_NODE', `第 ${index + 1} 个节点必须是对象`);
  }
  const id = typeof raw.id === 'string' ? raw.id.trim() : '';
  if (!ID_RE.test(id)) {
    throw new GraphError('INVALID_NODE_ID', `节点 id 非法：“${raw.id}”（需字母开头，仅字母数字、_、-）`, {
      index,
    });
  }
  if (seen.has(id)) throw new GraphError('DUPLICATE_NODE', `节点 id 重复：${id}`, { nodeId: id });
  seen.add(id);

  const type = raw.type || 'custom';
  if (!NODE_TYPES.has(type)) {
    throw new GraphError('INVALID_NODE_TYPE', `节点 ${id} 的类型 ${type} 不受支持`, { nodeId: id });
  }

  const dependsOn = raw.dependsOn ?? [];
  if (!Array.isArray(dependsOn) || dependsOn.some((d) => typeof d !== 'string')) {
    throw new GraphError('INVALID_DEPENDENCY', `节点 ${id} 的 dependsOn 必须是字符串数组`, { nodeId: id });
  }
  const uniqueDeps = [...new Set(dependsOn)];
  if (uniqueDeps.length !== dependsOn.length) {
    throw new GraphError('DUPLICATE_DEPENDENCY', `节点 ${id} 的 dependsOn 有重复项`, { nodeId: id });
  }
  if (uniqueDeps.includes(id)) {
    throw new GraphError('SELF_DEPENDENCY', `节点 ${id} 不能依赖自身`, { nodeId: id });
  }

  const retry = normalizeRetry(raw.retry, id);
  const skipWhen = raw.skipWhen == null ? null : raw.skipWhen;
  if (skipWhen != null) {
    const err = validateConditionSyntax(skipWhen);
    if (err) throw new GraphError('INVALID_CONDITION', `节点 ${id} 的跳过条件非法：${err}`, { nodeId: id });
  }

  return {
    id,
    type,
    label: typeof raw.label === 'string' && raw.label.trim() ? raw.label.trim() : id,
    dependsOn: uniqueDeps,
    skipWhen,
    retry,
    pauseAfter: raw.pauseAfter === true,
    config: raw.config && typeof raw.config === 'object' ? raw.config : {},
  };
}

function normalizeRetry(raw, nodeId) {
  const retry = { maxAttempts: 1, backoffMs: 0, backoffFactor: 1, maxBackoffMs: 30_000 };
  if (raw == null) return retry;
  if (typeof raw !== 'object') {
    throw new GraphError('INVALID_RETRY', `节点 ${nodeId} 的 retry 必须是对象`, { nodeId });
  }
  if (raw.maxAttempts != null) {
    if (!Number.isInteger(raw.maxAttempts) || raw.maxAttempts < 1 || raw.maxAttempts > 20) {
      throw new GraphError('INVALID_RETRY', `节点 ${nodeId} 的 retry.maxAttempts 需为 1..20 的整数`, { nodeId });
    }
    retry.maxAttempts = raw.maxAttempts;
  }
  for (const key of ['backoffMs', 'backoffFactor', 'maxBackoffMs']) {
    if (raw[key] != null) {
      if (typeof raw[key] !== 'number' || raw[key] < 0) {
        throw new GraphError('INVALID_RETRY', `节点 ${nodeId} 的 retry.${key} 必须是非负数`, { nodeId });
      }
      retry[key] = raw[key];
    }
  }
  return retry;
}

/**
 * Kahn's algorithm. Nodes at the same layer are independent (parallel branches).
 * Throws CYCLE with the detected cycle path when no progress is possible.
 */
export function topologicalOrder(nodes, byId) {
  const indegree = new Map(nodes.map((n) => [n.id, n.dependsOn.length]));
  const dependents = new Map(nodes.map((n) => [n.id, []]));
  for (const node of nodes) {
    for (const dep of node.dependsOn) dependents.get(dep).push(node.id);
  }
  const queue = nodes.filter((n) => indegree.get(n.id) === 0).map((n) => n.id);
  const order = [];
  while (queue.length) {
    const id = queue.shift();
    order.push(id);
    for (const next of dependents.get(id)) {
      indegree.set(next, indegree.get(next) - 1);
      if (indegree.get(next) === 0) queue.push(next);
    }
  }
  if (order.length !== nodes.length) {
    const remaining = nodes.filter((n) => !order.includes(n.id)).map((n) => n.id);
    const cycle = findCycle(remaining, byId);
    throw new GraphError('CYCLE', `流水线存在循环依赖：${cycle.join(' → ')}`, { cycle, nodes: remaining });  }
  return order;
}

function findCycle(remaining, byId) {
  const remainingSet = new Set(remaining);
  const state = new Map();
  const stack = [];
  let found = null;

  const visit = (id) => {
    if (found) return;
    state.set(id, 'visiting');
    stack.push(id);
    for (const dep of byId.get(id).dependsOn) {
      if (!remainingSet.has(dep)) continue;
      if (state.get(dep) === 'visiting') {
        found = [...stack.slice(stack.indexOf(dep)), dep];
        return;
      }
      if (!state.has(dep)) visit(dep);
      if (found) return;
    }
    stack.pop();
    state.set(id, 'done');
  };
  for (const id of remaining) {
    if (!state.has(id)) visit(id);
    if (found) break;
  }
  return found || remaining;
}

/**
 * Small condition grammar, validated statically:
 *   expr := term (('&&' | '||') term)*
 *   term := '!' term | '(' expr ')' | atom
 *   atom := nodeId.field [('==' | '!=') literal]
 *   field := status | output.<name> | input.<name> | meta.<name>
 *   literal := 'text' | "text" | 12 | -3.5 | true | false | bareWord
 *
 * A bare atom is true when the value is truthy. Node references must be
 * ancestors, so conditions only read settled upstream results.
 */
export function parseCondition(input) {
  if (typeof input !== 'string' || !input.trim()) throw new Error('条件为空');
  const tokens = [];
  let pos = 0;
  while (pos < input.length) {
    TOKEN_RE.lastIndex = pos;
    const m = TOKEN_RE.exec(input);
    if (!m) throw new Error(`无法解析：“${input.slice(pos, pos + 12).trim()}”`);
    const { op, str, num, id } = m.groups;
    if (op != null) tokens.push({ type: 'op', value: op });
    else if (str != null) tokens.push({ type: 'literal', value: unquote(str) });
    else if (num != null) tokens.push({ type: 'literal', value: Number(num) });
    else if (id === 'true') tokens.push({ type: 'literal', value: true });
    else if (id === 'false') tokens.push({ type: 'literal', value: false });
    else if (id != null) tokens.push({ type: 'word', value: id });
    pos = TOKEN_RE.lastIndex;
  }

  let i = 0;
  const peek = () => tokens[i];
  const next = () => tokens[i++];

  function parseExpr() {
    let left = parseTerm();
    while (peek()?.type === 'op' && (peek().value === '&&' || peek().value === '||')) {
      const op = next().value;
      left = { kind: 'binary', op, left, right: parseTerm() };
    }
    return left;
  }
  function parseTerm() {
    if (peek()?.type === 'op' && peek().value === '!') {
      next();
      return { kind: 'not', inner: parseTerm() };
    }
    if (peek()?.type === 'op' && peek().value === '(') {
      next();
      const inner = parseExpr();
      if (!(peek()?.type === 'op' && peek().value === ')')) throw new Error('缺少右括号');
      next();
      return inner;
    }
    return parseAtom();
  }
  function parseAtom() {
    const idToken = next();
    if (idToken?.type !== 'word') throw new Error(`期望节点 id，得到“${idToken?.value ?? 'EOF'}”`);
    const nodeId = idToken.value;
    if (!(peek()?.type === 'op' && peek().value === '.')) throw new Error(`节点 ${nodeId} 后缺少 “.”`);
    next();
    const fieldToken = next();
    if (fieldToken?.type !== 'word') throw new Error(`节点 ${nodeId} 后缺少字段`);
    const field = fieldToken.value;
    let sub = null;
    if (field === 'status') {
      sub = null;
    } else if (field === 'output' || field === 'input' || field === 'meta') {
      if (!(peek()?.type === 'op' && peek().value === '.')) {
        throw new Error(`${nodeId}.${field} 后缺少字段名`);
      }
      next();
      const subToken = next();
      if (subToken?.type !== 'word') throw new Error(`${nodeId}.${field} 后缺少字段名`);
      sub = subToken.value;
    } else {
      throw new Error(`未知字段 ${nodeId}.${field}（可用 status、output.*、input.*、meta.*）`);
    }
    let cmp = null;
    let literal = null;
    if (peek()?.type === 'op' && (peek().value === '==' || peek().value === '!=')) {
      cmp = next().value;
      const litToken = next();
      if (!litToken) throw new Error('比较运算符后缺少字面量');
      if (litToken.type === 'literal') literal = litToken.value;
      else if (litToken.type === 'word') literal = litToken.value;
      else throw new Error('比较运算符后缺少字面量');
    }
    return { kind: 'atom', nodeId, field, sub, cmp, literal };
  }

  const ast = parseExpr();
  if (i !== tokens.length) throw new Error(`多余内容：“${tokens.slice(i).map((t) => t.value).join(' ')}”`);
  return ast;
}

function unquote(str) {
  const inner = str.slice(1, -1);
  return inner.replace(/\\(.)/g, '$1');
}

function validateConditionSyntax(expr) {
  try {
    parseCondition(expr);
    return null;
  } catch (err) {
    return err.message;
  }
}

export function conditionReferences(expr) {
  const ast = typeof expr === 'string' ? safeParse(expr) : expr;
  const refs = [];
  walk(ast);
  return refs;

  function walk(n) {
    if (!n) return;
    if (n.kind === 'atom') refs.push({ kind: 'node', nodeId: n.nodeId, field: n.field, sub: n.sub });
    if (n.kind === 'not') walk(n.inner);
    if (n.kind === 'binary') {
      walk(n.left);
      walk(n.right);
    }
  }
}

function safeParse(expr) {
  try {
    return parseCondition(expr);
  } catch {
    return null;
  }
}

/**
 * Evaluates a condition against a node snapshot lookup:
 * (nodeId) => { status, output, input, meta }
 */
export function evaluateCondition(expr, lookup) {
  const ast = typeof expr === 'string' ? parseCondition(expr) : expr;
  return Boolean(evalNode(ast));

  function evalNode(n) {
    if (n.kind === 'not') return !evalNode(n.inner);
    if (n.kind === 'binary') {
      return n.op === '&&' ? evalNode(n.left) && evalNode(n.right) : evalNode(n.left) || evalNode(n.right);
    }
    const snapshot = lookup(n.nodeId) || {};
    let value;
    if (n.field === 'status') value = snapshot.status ?? 'pending';
    else value = snapshot[n.field]?.[n.sub] ?? null;
    if (n.cmp === '==') return value === n.literal;
    if (n.cmp === '!=') return value !== n.literal;
    return Boolean(value);
  }
}
