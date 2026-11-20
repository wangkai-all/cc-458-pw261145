const api = {
  async get(path) {
    const res = await fetch(path);
    return handle(res);
  },
  async post(path, body) {
    const res = await fetch(path, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body ?? {}),
    });
    return handle(res);
  },
};

async function handle(res) {
  const text = await res.text();
  let body = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = { raw: text };
  }
  if (!res.ok) {
    const err = new Error(body?.message || body?.error || `请求失败（${res.status}）`);
    err.status = res.status;
    err.body = body;
    throw err;
  }
  return body;
}

const STATUS_META = {
  pending: { label: '等待', glyph: '·' },
  running: { label: '运行中', glyph: '▶' },
  succeeded: { label: '成功', glyph: '✓' },
  skipped: { label: '跳过', glyph: '⤼' },
  failed: { label: '失败', glyph: '✕' },
  blocked: { label: '阻塞', glyph: '⊘' },
  cancelled: { label: '已取消', glyph: '■' },
  paused: { label: '已暂停', glyph: '❚❚' },
};

const RUN_STATUS_LABEL = {
  running: '运行中',
  paused: '已暂停',
  succeeded: '成功',
  failed: '失败',
  cancelled: '已取消',
};

const EVENT_SEVERITY = {
  node_failed: 'error',
  run_failed: 'error',
  scheduler_error: 'error',
  node_retrying: 'warn',
  node_blocked: 'warn',
  paused: 'warn',
  node_interrupted: 'warn',
  recovered: 'warn',
  node_succeeded: 'good',
  run_succeeded: 'good',
  node_skipped: 'muted',
  node_started: 'muted',
  node_cancelled: 'muted',
  effect_committed: 'good',
  effect_adopted: 'good',
};

const state = {
  runId: null,
  detail: null,
  selectedNodeId: null,
  filterSelected: false,
  stream: null,
  fixtures: [],
  runs: [],
  positions: new Map(),
};

const $ = (sel) => document.querySelector(sel);
const el = (tag, cls, text) => {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text != null) node.textContent = text;
  return node;
};

const fmtTime = (ts) => {
  if (!ts) return '—';
  const d = new Date(ts);
  return d.toLocaleTimeString('zh-CN', { hour12: false });
};
const fmtDateTime = (ts) => {
  if (!ts) return '—';
  const d = new Date(ts);
  return `${d.getMonth() + 1}-${String(d.getDate()).padStart(2, '0')} ${fmtTime(ts)}`;
};
const fmtDuration = (ms) => {
  if (ms == null) return '—';
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
};

function toast(message, kind = '') {
  const node = $('#toast');
  node.textContent = message;
  node.className = `toast ${kind}`;
  node.hidden = false;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => {
    node.hidden = true;
  }, 3600);
}

function statusOrb(status, sizeClass = '') {
  const meta = STATUS_META[status] ?? { label: status, glyph: '?' };
  const span = el('span', `status-orb s-${status} ${sizeClass}`);
  span.textContent = meta.glyph;
  span.setAttribute('aria-label', meta.label);
  span.title = meta.label;
  return span;
}

// ---------------------------------------------------------------- fixtures --
async function loadFixtures() {
  state.fixtures = await api.get('/api/fixtures');
  renderFixtures();
}

function renderFixtures() {
  const wrap = $('#fixtures');
  wrap.replaceChildren();
  for (const fixture of state.fixtures) {
    const btn = el('button', 'fixture');
    btn.type = 'button';
    const ok = fixture.validation.ok;
    if (!ok) btn.classList.add('fixture-invalid');
    btn.title = ok
      ? '在全新运行中执行该夹具'
      : `${fixture.validation.code}：${fixture.validation.message}`;
    btn.append(el('span', 'fixture-name', fixture.name));
    const flag = el('span', 'badge ' + (ok ? 'ok' : 'err'), ok ? '可运行' : fixture.validation.code);
    btn.append(flag);
    btn.append(el('span', 'fixture-meta', `${fixture.nodeCount} 个节点 · ${fixture.file}`));
    btn.addEventListener('click', () => startFixture(fixture));
    wrap.append(btn);
  }
}

async function startFixture(fixture) {
  if (!fixture.validation.ok) {
    const details = fixture.validation.details?.cycle
      ? `循环路径：${fixture.validation.details.cycle.join(' → ')}`
      : fixture.validation.details?.reference
        ? `非法引用：${fixture.validation.details.reference}`
        : '';
    toast(`${fixture.validation.message}${details ? `（${details}）` : ''}`, 'error');
    return;
  }
  try {
    const res = await api.post('/api/runs', { fixture: fixture.file });
    toast(`已创建运行 ${res.runId}`);
    await refreshRuns();
    await selectRun(res.runId);
  } catch (err) {
    toast(err.message, 'error');
  }
}

// -------------------------------------------------------------------- runs --
async function refreshRuns() {
  state.runs = await api.get('/api/runs');
  renderRuns();
}

function renderRuns() {
  const list = $('#runs');
  list.replaceChildren();
  if (!state.runs.length) {
    list.append(el('li', 'muted', '暂无运行'));
    return;
  }
  for (const run of state.runs) {
    const li = el('li');
    const btn = el('button', 'run');
    btn.type = 'button';
    if (run.id === state.runId) btn.setAttribute('aria-current', 'true');
    btn.append(statusOrb(run.status));
    const name = el('span', 'run-name', run.pipeline_name);
    name.title = run.id;
    btn.append(name);
    btn.append(el('span', 'run-time', fmtDateTime(run.created_at)));
    btn.append(el('span', 'run-id muted mono', run.id.slice(-6)));
    if (run.gate) btn.append(el('span', 'run-gate', '⏸ 待恢复'));
    btn.addEventListener('click', () => selectRun(run.id));
    li.append(btn);
    list.append(li);
  }
}

async function selectRun(runId) {
  state.runId = runId;
  state.selectedNodeId = null;
  state.positions.clear();
  if (state.stream) {
    state.stream.close();
    state.stream = null;
  }
  renderRuns();
  await fetchDetail();
  startStream();
}

async function fetchDetail({ quiet = false } = {}) {
  if (!state.runId) return;
  try {
    state.detail = await api.get(`/api/runs/${state.runId}`);
    renderAll();
  } catch (err) {
    if (!quiet) toast(err.message, 'error');
  }
}

function startStream() {
  if (!state.runId) return;
  const source = new EventSource(`/api/runs/${state.runId}/stream`);
  state.stream = source;
  source.addEventListener('snapshot', (event) => {
    state.detail = JSON.parse(event.data);
    renderAll();
    refreshRuns().catch(() => {});
  });
  source.addEventListener('events', (event) => {
    const payload = JSON.parse(event.data);
    if (!state.detail) return;
    state.detail.events = [...state.detail.events, ...payload.events];
    state.detail.run.status = payload.runStatus;
    state.detail.run.gate = payload.gate;
    state.detail.run.updated_at = payload.updatedAt;
    for (const ev of payload.events) applyEventToNode(ev);
    renderBanner();
    renderTiles();
    renderDagStatus();
    renderEvents(payload.events);
    renderInspector();
    refreshRuns().catch(() => {});
  });
  source.addEventListener('heartbeat', (event) => {
    const payload = JSON.parse(event.data);
    if (!state.detail) return;
    state.detail.run.status = payload.runStatus;
    state.detail.run.gate = payload.gate;
    state.detail.run.updated_at = payload.updatedAt;
    renderBanner();
    renderRuns();
  });
  source.addEventListener('closed', () => {
    source.close();
    if (state.stream === source) state.stream = null;
    fetchDetail({ quiet: true }).catch(() => {});
  });
  source.addEventListener('deleted', () => {
    toast('运行已被移除', 'error');
    source.close();
  });
  source.onerror = () => {
    // EventSource retries automatically; surface a hint only once.
    if (!state.streamDown) {
      state.streamDown = true;
      setTimeout(() => {
        state.streamDown = false;
      }, 4000);
    }
  };
}

function applyEventToNode(ev) {
  if (!ev.node_id || !state.detail) return;
  const node = state.detail.nodes.find((n) => n.id === ev.node_id);
  if (!node) return;
  const map = {
    node_started: 'running',
    node_succeeded: 'succeeded',
    node_failed: 'failed',
    node_skipped: 'skipped',
    node_blocked: 'blocked',
    node_cancelled: 'cancelled',
  };
  if (map[ev.type]) node.status = map[ev.type];
  if (ev.type === 'node_failed') node.error = ev.message;
  if (ev.type === 'node_skipped') {
    node.skipReason = ev.detail?.reason ?? node.skipReason;
  }
  if (ev.type === 'log') {
    node.logs = [...(node.logs ?? []), {
      at: ev.at,
      level: ev.detail?.level ?? 'info',
      message: ev.message,
      attempt: node.attempt ?? 1,
    }].slice(-200);
  }
  if (ev.type === 'effect_committed' || ev.type === 'effect_adopted') {
    node.effects = [...(node.effects ?? []), { effect_key: ev.message.split('：').at(-1), payload: ev.detail }];
  }
}

// ------------------------------------------------------------------ render --
function renderAll() {
  if (!state.detail) return;
  renderHeader();
  renderBanner();
  renderTiles();
  renderDag();
  renderInspector();
  renderEvents(state.detail.events);
}

function renderHeader() {
  const { run } = state.detail;
  $('#run-title').textContent = run.pipeline_name;
  const sub = [
    `运行 ${run.id}`,
    `状态 ${RUN_STATUS_LABEL[run.status] ?? run.status}`,
    `创建 ${fmtDateTime(run.created_at)}`,
    run.finished_at ? `耗时 ${fmtDuration(run.finished_at - run.created_at)}` : null,
  ].filter(Boolean);
  $('#run-sub').textContent = sub.join(' · ');
  renderActions();
}

function renderActions() {
  const { run } = state.detail;
  const actions = $('#actions');
  actions.replaceChildren();

  const mk = (label, opts, handler) => {
    const btn = el('button', `pill-btn ${opts.kind ?? ''}`, label);
    btn.type = 'button';
    btn.disabled = Boolean(opts.disabled);
    if (opts.title) btn.title = opts.title;
    btn.addEventListener('click', async () => {
      btn.disabled = true;
      try {
        await handler();
      } catch (err) {
        toast(err.message, 'error');
        btn.disabled = false;
      }
    });
    actions.append(btn);
    return btn;
  };

  const live = run.status === 'running';
  const paused = run.status === 'paused';

  if (live) {
    mk('暂停', { title: '不再调度新节点；运行中的节点先完成' }, () => act('pause'));
  } else if (paused) {
    mk('恢复运行', { kind: 'primary', title: '继续调度待执行节点' }, () => act('resume'));
  }
  if (live || paused) {
    mk('取消', { kind: 'danger', title: '中断运行中节点，未开始节点不再执行' }, () => act('cancel'));
  }
  if (run.status === 'failed' || run.status === 'cancelled' || run.status === 'succeeded') {
    const failedNode = state.detail.nodes.find((n) =>
      ['failed', 'blocked', run.status === 'cancelled' ? 'cancelled' : ''].includes(n.status),
    );
    if (failedNode) {
      mk(`重试自 ${failedNode.label}`, { title: '重跑该节点及其下游' }, () => retryFrom(failedNode.id));
    }
  }
  mk('刷新', {}, async () => {
    await fetchDetail();
    await refreshRuns();
  });
}

async function act(kind) {
  await api.post(`/api/runs/${state.runId}/${kind}`);
  await fetchDetail();
  await refreshRuns();
  if (kind === 'pause') toast('已请求暂停；运行中的节点会先完成');
  if (kind === 'resume') toast('已恢复运行');
  if (kind === 'cancel') toast('已取消运行');
}

function renderBanner() {
  const banner = $('#banner');
  const { run } = state.detail;
  banner.replaceChildren();
  banner.className = 'banner';
  if (run.status === 'paused') {
    banner.hidden = false;
    const gate = run.gate?.startsWith('pause_after:')
      ? `节点 ${run.gate.split(':')[1]} 到达人工暂停点`
      : '运行已人工暂停';
    banner.append(el('strong', '', '⏸ 等待人工恢复'));
    banner.append(el('span', '', `${gate}。已完成节点的副作用已持久化；恢复后从未完成处继续。`));
    banner.append(el('span', 'spacer'));
    const btn = el('button', 'pill-btn primary', '恢复运行');
    btn.type = 'button';
    btn.addEventListener('click', () => act('resume'));
    banner.append(btn);
    return;
  }
  if (run.status === 'failed') {
    banner.hidden = false;
    banner.classList.add('error');
    const failed = state.detail.nodes.filter((n) => n.status === 'failed').length;
    const blocked = state.detail.nodes.filter((n) => n.status === 'blocked').length;
    banner.append(el('strong', '', '运行失败'));
    banner.append(el('span', '', `${failed} 个节点失败、${blocked} 个节点因输入不完整被阻塞。`));
    const target = state.detail.nodes.find((n) => n.status === 'failed' || n.status === 'blocked');
    if (target) {
      banner.append(el('span', 'spacer'));
      const btn = el('button', 'pill-btn', `从 ${target.label} 重试`);
      btn.type = 'button';
      btn.addEventListener('click', () => retryFrom(target.id));
      banner.append(btn);
    }
    return;
  }
  if (run.status === 'cancelled') {
    banner.hidden = false;
    banner.classList.add('neutral');
    banner.append(el('strong', '', '运行已取消'));
    banner.append(el('span', '', '已提交的副作用保留；可按需从某个节点重试。'));
    const target = state.detail.nodes.find((n) => n.status === 'cancelled');
    if (target) {
      banner.append(el('span', 'spacer'));
      const btn = el('button', 'pill-btn', `从 ${target.label} 重试`);
      btn.type = 'button';
      btn.addEventListener('click', () => retryFrom(target.id));
      banner.append(btn);
    }
    return;
  }
  banner.hidden = true;
}

function renderTiles() {
  const { run, nodes } = state.detail;
  const wrap = $('#tiles');
  wrap.replaceChildren();
  const counts = {};
  for (const node of nodes) counts[node.status] = (counts[node.status] ?? 0) + 1;

  const statusTile = el('div', 'tile tile-status');
  statusTile.append(statusOrb(run.status, 'big'));
  const body = el('div', 'tile-body');
  body.append(el('div', 'tile-label', '运行状态'));
  body.append(el('div', 'tile-value', RUN_STATUS_LABEL[run.status] ?? run.status));
  const hintParts = [];
  if (run.status === 'paused') hintParts.push('等待人工恢复');
  else if (run.finished_at) hintParts.push(`总耗时 ${fmtDuration(run.finished_at - run.created_at)}`);
  else hintParts.push(`已运行 ${fmtDuration(Date.now() - run.created_at)}`);
  body.append(el('div', 'tile-hint', hintParts.join(' · ')));
  statusTile.append(body);
  wrap.append(statusTile);

  const done = (counts.succeeded ?? 0) + (counts.skipped ?? 0);
  const nodeTile = el('div', 'tile');
  nodeTile.append(el('div', 'tile-label', '节点完成'));
  const v = el('div', 'tile-value', `${done}`);
  v.append(el('small', '', `/ ${nodes.length}`));
  nodeTile.append(v);
  const seg = el('div', 'seg');
  for (const status of ['succeeded', 'running', 'failed', 'blocked', 'skipped', 'pending', 'cancelled']) {
    const count = counts[status] ?? 0;
    if (!count) continue;
    const part = el('span');
    part.style.flex = `${count}`;
    part.style.background = `var(--${status})`;
    part.title = `${STATUS_META[status].label} ${count}`;
    seg.append(part);
  }
  nodeTile.append(seg);
  wrap.append(nodeTile);

  const retryTile = el('div', 'tile');
  retryTile.append(el('div', 'tile-label', '失败 / 阻塞'));
  const failValue = el('div', 'tile-value', `${counts.failed ?? 0}`);
  failValue.append(el('small', '', `/ ${counts.blocked ?? 0}`));
  retryTile.append(failValue);
  const attempts = nodes.reduce((sum, n) => sum + Math.max(0, (n.attempt ?? 1) - 1), 0);
  retryTile.append(
    el(
      'div',
      'tile-hint',
      `共执行 ${nodes.reduce((s, n) => s + (n.attempt ?? 0), 0)} 次 · 重试 ${attempts} 次`,
    ),
  );
  wrap.append(retryTile);

  const effectTile = el('div', 'tile');
  effectTile.append(el('div', 'tile-label', '已确认副作用'));
  effectTile.append(el('div', 'tile-value', `${state.detail.effects.length}`));
  effectTile.append(el('div', 'tile-hint', '已提交的外部写入，重启后不会重复'));
  wrap.append(effectTile);

  const skippedTile = el('div', 'tile');
  skippedTile.append(el('div', 'tile-label', '条件跳过'));
  skippedTile.append(el('div', 'tile-value', `${counts.skipped ?? 0}`));
  skippedTile.append(el('div', 'tile-hint', '满足跳过条件或上游被跳过'));
  wrap.append(skippedTile);
}

// --------------------------------------------------------------------- DAG --
function layerNodes(nodes) {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const depth = new Map();
  const visit = (id) => {
    if (depth.has(id)) return depth.get(id);
    const node = byId.get(id);
    if (!node) return 0;
    const d = node.dependsOn.length
      ? Math.max(...node.dependsOn.map((dep) => visit(dep))) + 1
      : 0;
    depth.set(id, d);
    return d;
  };
  const layers = [];
  for (const node of nodes) {
    const d = visit(node.id);
    (layers[d] ??= []).push(node);
  }
  for (const layer of layers) {
    layer.sort((a, b) => a.id.localeCompare(b.id));
  }
  return { layers, depth, byId };
}

function renderDag() {
  const { nodes } = state.detail;
  const dag = $('#dag');
  dag.replaceChildren();
  if (!nodes.length) return;

  const { layers, depth } = layerNodes(nodes);
  // measure the container, not the (possibly already widened) scroll box, so
  // re-renders converge instead of ratcheting the width up each time
  const wrapWidth = dag.parentElement?.clientWidth ?? 0;
  const avail = Math.max(300, wrapWidth - 30);
  const NODE_H = 92;
  const GAP_Y = 20;
  const PAD = 14;
  const minNodeW = 130;
  const maxNodeW = 176;
  const gapFor = (n) => Math.max(10, Math.min(72, Math.round(n * 0.04)));
  let GAP_X = gapFor(avail);
  let NODE_W = Math.min(
    maxNodeW,
    Math.max(minNodeW, Math.floor((avail - PAD * 2 - (layers.length - 1) * GAP_X) / layers.length)),
  );
  let width = PAD * 2 + layers.length * NODE_W + (layers.length - 1) * GAP_X;
  // if the nodes still do not fit, trade edge spacing for node width before
  // falling back to horizontal scroll
  if (width > avail && layers.length > 2) {
    const spare = (avail - PAD * 2 - layers.length * NODE_W) / (layers.length - 1);
    GAP_X = Math.max(8, Math.floor(spare));
    width = PAD * 2 + layers.length * NODE_W + (layers.length - 1) * GAP_X;
  }
  const tight = NODE_W < 150;
  dag.classList.toggle('tight', tight);
  const tallest = Math.max(...layers.map((l) => l.length));
  const contentH = PAD * 2 + tallest * NODE_H + Math.max(0, tallest - 1) * GAP_Y;
  const wrapHeight = dag.parentElement?.clientHeight ?? 0;
  const height = Math.max(contentH, wrapHeight ? wrapHeight - 56 : 0, 240);
  dag.style.width = `${Math.max(width, 300)}px`;
  dag.style.height = `${height}px`;

  const positions = new Map();
  layers.forEach((layer, li) => {
    const x = PAD + li * (NODE_W + GAP_X);
    const y0 = (height - (layer.length * NODE_H + (layer.length - 1) * GAP_Y)) / 2;
    layer.forEach((node, ni) => {
      positions.set(node.id, { x, y: y0 + ni * (NODE_H + GAP_Y) });
    });
  });
  state.positions = positions;
  state.nodeW = NODE_W;
  state.nodeH = NODE_H;

  const svgNS = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(svgNS, 'svg');
  svg.classList.add('edges');
  svg.setAttribute('width', width);
  svg.setAttribute('height', height);
  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  dag.append(svg);

  const { byId } = { byId: new Map(nodes.map((n) => [n.id, n])) };
  for (const node of nodes) {
    const to = positions.get(node.id);
    for (const depId of node.dependsOn) {
      const from = positions.get(depId);
      const dep = byId.get(depId);
      if (!from || !to) continue;
      const x1 = from.x + NODE_W;
      const y1 = from.y + NODE_H / 2;
      const x2 = to.x;
      const y2 = to.y + NODE_H / 2;
      const mid = (x1 + x2) / 2;
      const path = document.createElementNS(svgNS, 'path');
      path.setAttribute('d', `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`);
      path.setAttribute('fill', 'none');
      path.setAttribute('stroke', edgeColor(dep, node, state.detail));
      path.setAttribute('stroke-width', '1.6');
      if (node.status === 'pending' || dep.status === 'skipped') {
        path.setAttribute('stroke-dasharray', '4 4');
      }
      if (dep.status === 'running') path.setAttribute('stroke-dasharray', '6 4');
      path.dataset.from = depId;
      path.dataset.to = node.id;
      svg.append(path);
      // arrowhead at the midpoint of the curve, kept subtle
      const dot = document.createElementNS(svgNS, 'circle');
      dot.setAttribute('cx', x2);
      dot.setAttribute('cy', y2);
      dot.setAttribute('r', '2.6');
      dot.setAttribute('fill', edgeColor(dep, node, state.detail));
      svg.append(dot);
    }
  }

  for (const node of nodes) {
    const pos = positions.get(node.id);
    const btn = el('button', 'dag-node');
    btn.type = 'button';
    btn.style.left = `${pos.x}px`;
    btn.style.top = `${pos.y}px`;
    btn.style.width = `${NODE_W}px`;
    btn.style.height = `${NODE_H}px`;
    btn.dataset.nodeId = node.id;
    btn.setAttribute('aria-selected', String(state.selectedNodeId === node.id));
    if (node.status === 'failed') btn.style.borderColor = '#6a3030';
    if (node.status === 'blocked') btn.style.borderColor = '#5b3a2a';

    const top = el('div', 'node-top');
    top.append(statusOrb(node.status));
    top.append(el('span', 'node-label', node.label));
    btn.append(top);
    btn.append(el('div', 'node-id', tight ? node.id : `${node.type} · ${node.id}`));

    const badges = el('div', 'node-badges');
    if (node.attempt > 1) badges.append(el('span', 'badge warn', `第 ${node.attempt} 次`));
    if (node.status === 'skipped') {
      badges.append(
        el('span', 'badge', node.skipReason === 'condition' ? '条件跳过' : '上游跳过'),
      );
    }
    if (node.effects?.length) badges.append(el('span', 'badge ok', `副作用 ×${node.effects.length}`));
    if (node.status === 'blocked') badges.append(el('span', 'badge err', '输入不完整'));
    if (node.status === 'failed') badges.append(el('span', 'badge err', '失败'));
    if (!badges.childElementCount) badges.append(el('span', 'badge', STATUS_META[node.status]?.label ?? node.status));
    badges.title = [...badges.children].map((b) => b.textContent).join(' · ');
    btn.append(badges);

    btn.title = `${node.label}（${node.id}）\n状态：${STATUS_META[node.status]?.label ?? node.status}\n类型：${node.type}`;
    btn.addEventListener('click', () => selectNode(node.id));
    btn.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        selectNode(node.id);
      }
    });
    dag.append(btn);
  }

  const hint = $('#dag-hint');
  hint.textContent = `${layers.length} 层 · ${nodes.length} 个节点 · 实线=已完成输入，虚线=等待/跳过`;
}

function edgeColor(from, to, detail) {
  if (from.status === 'failed' || from.status === 'blocked') return '#6a3030';
  if (to.status === 'blocked') return '#5b3a2a';
  if (to.status === 'skipped' || from.status === 'skipped') return '#3a3a36';
  if (from.status === 'succeeded' && to.status === 'succeeded') return '#2f6b34';
  if (from.status === 'running') return '#2a78d6';
  return '#3a3a36';
}

function renderDagStatus() {
  if (!state.detail) return;
  for (const node of state.detail.nodes) {
    const btn = document.querySelector(`.dag-node[data-node-id="${cssEscape(node.id)}"]`);
    if (!btn) continue;
    const orb = btn.querySelector('.status-orb');
    const meta = STATUS_META[node.status] ?? { label: node.status, glyph: '?' };
    orb.className = `status-orb s-${node.status}`;
    orb.textContent = meta.glyph;
    orb.title = meta.label;
    orb.setAttribute('aria-label', meta.label);
    btn.setAttribute('aria-selected', String(state.selectedNodeId === node.id));
  }
  // refresh edges too (cheap on a small DAG)
  renderDag();
}

function cssEscape(value) {
  return String(value).replace(/["\\]/g, '\\$&');
}

// --------------------------------------------------------------- inspector --
function selectNode(nodeId) {
  state.selectedNodeId = nodeId;
  renderDag();
  renderInspector();
  renderEvents(state.detail?.events ?? []);
}

function renderInspector() {
  const wrap = $('#inspector');
  const { nodes } = state.detail;
  const node = nodes.find((n) => n.id === state.selectedNodeId);
  wrap.replaceChildren();
  if (!node) {
    const empty = el('div', 'empty-inspector');
    empty.append(el('p', 'muted', '点击 DAG 节点查看日志摘要、副作用记录与重试范围。'));
    wrap.append(empty);
    return;
  }

  const head = el('div', 'insp-head');
  head.append(statusOrb(node.status));
  const title = el('div', 'insp-title');
  title.append(el('strong', '', node.label));
  title.append(el('span', '', `${node.type} · ${node.id}`));
  head.append(title);
  wrap.append(head);

  const kv = el('dl', 'kv');
  addKv(kv, '状态', STATUS_META[node.status]?.label ?? node.status);
  addKv(kv, '尝试次数', `${node.attempt ?? 0}${node.retry?.maxAttempts ? ` / ${node.retry.maxAttempts}` : ''}`);
  addKv(kv, '开始', node.startedAt ? fmtDateTime(node.startedAt) : '—');
  addKv(kv, '耗时', node.durationMs != null ? fmtDuration(node.durationMs) : '—');
  if (node.dependsOn.length) addKv(kv, '上游', node.dependsOn.join(', '));
  if (node.skipWhen) addKv(kv, '跳过条件', node.skipWhen);
  wrap.append(kv);

  if (node.error && ['failed', 'blocked'].includes(node.status)) {
    wrap.append(el('div', 'error-box', node.error));
  } else if (node.status === 'skipped') {
    const reason =
      node.skipReason === 'condition'
        ? `满足跳过条件：${node.skipWhen}`
        : '上游分支已跳过，节点不再执行';
    wrap.append(el('div', 'skip-box', `⤼ ${reason}`));
  }

  if (node.output) {
    const section = el('div', 'insp-section');
    section.append(el('h3', '', '输出摘要'));
    const out = el('dl', 'kv');
    for (const [key, value] of Object.entries(node.output)) {
      addKv(out, key, typeof value === 'object' ? JSON.stringify(value) : String(value));
    }
    section.append(out);
    wrap.append(section);
  }

  if (node.effects?.length) {
    const section = el('div', 'insp-section');
    section.append(el('h3', '', '已确认副作用'));
    for (const effect of node.effects) {
      const div = el('div', 'effect');
      div.append(document.createTextNode(effect.effect_key ?? effect.key ?? 'effect'));
      div.append(el('span', '', `提交于 ${fmtDateTime(effect.committed_at ?? effect.at ?? Date.now())}`));
      section.append(div);
    }
    wrap.append(section);
  }

  if (['failed', 'blocked'].includes(node.status) || (node.status === 'cancelled')) {
    const scope = state.detail.retryScopes[node.id];
    if (scope?.length) {
      const section = el('div', 'insp-section');
      section.append(el('h3', '', '重试范围'));
      const row = el('div', 'scope');
      scope.forEach((id, i) => {
        if (i) row.append(el('span', 'arrow', '→'));
        const chip = el('span', `chip${id === node.id ? ' target' : ''}`, id);
        row.append(chip);
      });
      section.append(row);
      const note = el('p', 'muted', '');
      note.style.fontSize = '11px';
      note.textContent =
        '重跑该节点与全部下游；已确认的副作用会被复用，不会重复写入外部系统。';
      section.append(note);
      wrap.append(section);
    }
    const actions = el('div', 'insp-actions');
    const btn = el('button', 'pill-btn primary', '重试此范围');
    btn.type = 'button';
    btn.addEventListener('click', () => retryFrom(node.id));
    actions.append(btn);
    wrap.append(actions);
  } else if (node.status === 'cancelled') {
    const actions = el('div', 'insp-actions');
    const btn = el('button', 'pill-btn primary', '重试此范围');
    btn.type = 'button';
    btn.addEventListener('click', () => retryFrom(node.id));
    actions.append(btn);
    wrap.append(actions);
  }

  const logSection = el('div', 'insp-section');
  logSection.append(el('h3', '', '日志摘要'));
  const logs = node.logs ?? [];
  if (!logs.length) {
    logSection.append(el('p', 'muted', '暂无日志'));
  } else {
    const list = el('ul', 'log-summary');
    logs.slice(-60).forEach((entry, idx, arr) => {
      const li = el('li');
      if (idx > 0 && arr[idx - 1].attempt !== entry.attempt) li.classList.add('attempt');
      li.append(el('span', 't', fmtTime(entry.at)));
      li.append(el('span', `lvl ${entry.level}`, entry.level));
      li.append(el('span', 'msg', entry.message));
      list.append(li);
    });
    logSection.append(list);
  }
  wrap.append(logSection);
}

function addKv(dl, key, value) {
  dl.append(el('dt', '', key));
  dl.append(el('dd', '', value));
}

async function retryFrom(nodeId) {
  try {
    const res = await api.post(`/api/runs/${state.runId}/retry`, { nodeId });
    toast(`已重跑 ${res.scope.length} 个节点：${res.scope.join(' → ')}`);
    state.detail = await api.get(`/api/runs/${state.runId}`);
    renderAll();
    await refreshRuns();
    startStream();
  } catch (err) {
    toast(err.message, 'error');
  }
}

// ------------------------------------------------------------------ events --
function renderEvents(newEvents = null) {
  const list = $('#event-list');
  const events = state.detail?.events ?? [];
  const filter = state.filterSelected && state.selectedNodeId;
  const shown = filter ? events.filter((ev) => ev.node_id === state.selectedNodeId) : events;
  const visible = shown.slice(-160).reverse();
  list.replaceChildren();
  if (!visible.length) {
    list.append(el('li', 'muted', filter ? '该节点暂无事件' : '暂无事件'));
    return;
  }
  const freshIds = new Set((newEvents ?? []).map((ev) => ev.seq));
  for (const ev of visible) {
    const li = el('li');
    const sev = EVENT_SEVERITY[ev.type] ?? '';
    if (sev) li.classList.add(`sev-${sev}`);
    if (freshIds.has(ev.seq)) li.classList.add('fresh');
    li.append(el('span', 'ts', fmtTime(ev.at)));
    li.append(el('span', 'who', ev.node_id ?? 'run'));
    li.append(el('span', 'what', ev.message));
    list.append(li);
  }
}

// ---------------------------------------------------------------- controls --
function bindControls() {
  const checkbox = $('#filter-node');
  checkbox.addEventListener('change', () => {
    state.filterSelected = checkbox.checked;
    renderEvents();
  });
  document.addEventListener('keydown', (event) => {
    if (event.target.matches('input, textarea')) return;
    if (event.key === 'Escape') {
      state.selectedNodeId = null;
      renderDag();
      renderInspector();
    }
    if (event.key === 'r' && state.runId) fetchDetail().catch(() => {});
  });
}

function renderLegend() {
  const legend = $('#legend');
  legend.replaceChildren();
  for (const status of ['pending', 'running', 'succeeded', 'skipped', 'failed', 'blocked', 'cancelled']) {
    const span = el('span');
    const dot = el('i');
    dot.style.background = `var(--${status})`;
    span.append(dot, document.createTextNode(STATUS_META[status].label));
    legend.append(span);
  }
}

async function loadEngineInfo() {
  try {
    const info = await api.get('/api/health');
    $('#engine-info').textContent = `并发 ${info.concurrency} · ${info.recovered.length} 个运行已恢复`;
  } catch {
    $('#engine-info').textContent = '引擎不可用';
  }
}

async function boot() {
  bindControls();
  renderLegend();
  await Promise.all([loadFixtures(), refreshRuns(), loadEngineInfo()]);
  const params = new URLSearchParams(location.search);
  const initial = params.get('run');
  if (initial) {
    await selectRun(initial);
  } else if (state.runs.length) {
    await selectRun(state.runs[0].id);
  }
  setInterval(() => {
    if (!state.stream) refreshRuns().catch(() => {});
  }, 4000);
}

boot().catch((err) => toast(err.message, 'error'));
