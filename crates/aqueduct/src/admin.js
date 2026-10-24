const $ = id => document.getElementById(id);
// The token is sent only as an Authorization header. Only a one-time #token=... in the address bar
// is accepted: it is moved into sessionStorage and stripped from the URL so it never reaches
// history, Referer or screenshots. A ?token= in the page URL is ignored (and stripped), because
// loading such a link would already have sent the secret in the GET request line.
const token = (() => {
  const u = new URL(location.href);
  const h = new URLSearchParams(u.hash.slice(1));
  const given = h.get('token');
  if (given || u.searchParams.has('token')) {
    u.searchParams.delete('token');
    h.delete('token');
    u.hash = h.toString();
    history.replaceState(null, '', u.pathname + u.search + u.hash);
  }
  if (given) {
    try { sessionStorage.setItem('aqueduct_token', given); } catch (_) { /* storage blocked: keep in memory */ }
    return given;
  }
  try { return sessionStorage.getItem('aqueduct_token'); } catch (_) { return null; }
})();
const authInit = extra => Object.assign({}, extra, token ? { headers: { Authorization: 'Bearer ' + token } } : {});
const fmt = n => n.toLocaleString('en-US');
const dur = s => s < 60 ? s + 's' : s < 3600 ? Math.floor(s / 60) + 'm ' + (s % 60) + 's' : Math.floor(s / 3600) + 'h ' + Math.floor(s % 3600 / 60) + 'm';
const ms = v => v < 10 ? v.toFixed(2) + ' ms' : v < 1000 ? v.toFixed(0) + ' ms' : (v / 1000).toFixed(1) + ' s';
const bytes = n => n < 1024 ? n + ' B' : n < 1048576 ? (n / 1024).toFixed(1) + ' KiB' : n < 1073741824 ? (n / 1048576).toFixed(1) + ' MiB' : (n / 1073741824).toFixed(2) + ' GiB';
const esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// Rates, the chart and recent sink latency are trailing averages over this many 1 s snapshots;
// counters, lag and max latency stay exact (lag is an alarm signal, a mean would hide a stall).
const SMOOTH_WINDOW = 15;
const avg = a => a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0;
const smooth = (h, w = SMOOTH_WINDOW) => h.map((_, i) => avg(h.slice(Math.max(0, i - w + 1), i + 1)));
// Sink latency samples, one per snapshot in which events were delivered (sink_last_ms only
// changes then). Cleared on reconnect so values from before an outage are not blended in.
let sinkSamples = [], lastTotal = null;
// The end-to-end lag tile reads "idle" when the source is caught up and nothing was delivered for this long.
const IDLE_AFTER_MS = 10000;
const resetSmoothing = () => { sinkSamples = []; lastTotal = null; };

const STATE_DOT = { streaming: 'bg-green', starting: 'bg-amber', failed: 'bg-red', stopped: 'bg-ink-300' };
const DOT_BASE = 'h-1.5 w-1.5 rounded-full ';

let lastData = null;
// Op colours (insert, update, delete, truncate, read) for the cards' op strips, the legend and the per-op chart lines:
// they differ in lightness as well as hue, so neighbouring shares stay apart in a thin strip.
const OPLINE = ['#22c55e', '#38bdf8', '#ef4444', '#a855f7', '#facc15'];
// Stepline points: hold each value until the next sample, then jump.
const stepPts = (h, x, y) => h.map((v, i) => (i ? x(i).toFixed(1) + ',' + y(h[i - 1]).toFixed(1) + ' ' : '') + x(i).toFixed(1) + ',' + y(v).toFixed(1)).join(' ');
// Draws `series` ({ op, h }, coloured colors[op]) into the SVG `el` (viewBox 0 0 800 H): one step line each, or with
// `stacked` filled step bands on top of each other in series order (aligned on their newest sample), so the top edge
// is their sum.
function chart(series, el, colors = OPLINE, H = 220, stacked = false) {
  const W = 800, PL = 34, PR = 8, PT = 10, PB = 8;
  const len = Math.max(0, ...series.map(s => s.h.length));
  if (stacked) {
    let below = new Array(len).fill(0);
    series = series.map(({ op, h }) => {
      const top = below.map((v, i) => v + (h[i - (len - h.length)] || 0));
      const s = { op, h: top, below };
      below = top;
      return s;
    });
  }
  const max = Math.max(5, ...series.flatMap(s => s.h));
  const n = Math.max(len, 2);
  const x = i => PL + (W - PL - PR) * i / (n - 1);
  const y = v => H - PB - (H - PT - PB) * v / max;
  let g = '';
  for (let i = 0; i <= 4; i++) {
    const yy = PT + (H - PT - PB) * i / 4;
    g += `<line x1="${PL}" x2="${W - PR}" y1="${yy}" y2="${yy}" stroke="#2f343c" stroke-width="1" vector-effect="non-scaling-stroke"/>`;
    g += `<text x="${PL - 6}" y="${yy + 3}" text-anchor="end" fill="#738091" font-size="10" font-family="ui-monospace,Consolas,monospace">${Math.round(max * (1 - i / 4))}</text>`;
  }
  if (len > 1) {
    // Stepline per op: hold each value until the next sample, then jump.
    for (const { op, h, below } of series) {
      if (below) g += `<polygon points="${stepPts(h, x, y)} ${stepPts(below, x, y).split(' ').reverse().join(' ')}" fill="${colors[op]}" fill-opacity="0.45" stroke="none"/>`;
      g += `<polyline points="${stepPts(h, x, y)}" fill="none" stroke="${colors[op]}" stroke-width="1.5" vector-effect="non-scaling-stroke"/>`;
    }
  }
  el.innerHTML = g;
}

// Big-number tile text; the colour class is appended.
const TILE = 'mt-2 font-mono text-3xl font-medium tabular-nums ';

// Snapshot card: hidden until the source reports one, then shows the running or last snapshot.
const SNAP_BADGE = { running: 'bg-blue/15 text-blue', finished: 'bg-green/15 text-green', cancelled: 'bg-ink-700 text-ink-200' };
function renderSnapshot(d) {
  const s = d.snapshot;
  $('snapshot').hidden = !s;
  if (!s) return;
  $('snapstate').textContent = s.state;
  $('snapstate').className = 'rounded-sm px-1.5 py-0.5 font-mono text-[10px] uppercase tracking-wider ' + (SNAP_BADGE[s.state] || 'bg-ink-700 text-ink-200');
  $('snapid').textContent = s.id;
  $('snapcancel').hidden = s.state !== 'running';
  $('snaptables').textContent = s.tables_done + ' / ' + s.tables.length;
  $('snaptable').textContent = s.table ? 'copying ' + s.table : s.state;
  $('snaprows').textContent = fmt(s.rows);
  const pct = s.rows_estimated ? Math.min(100, 100 * s.rows / s.rows_estimated) : (s.state === 'finished' ? 100 : 0);
  $('snapbar').style.width = pct + '%';
  $('snapsub').textContent = s.rows_estimated ? 'of about ' + fmt(s.rows_estimated) + ' (planner estimate) · this process only' : 'no row estimate (run ANALYZE) · this process only';
  $('snapskipped').hidden = !s.skipped.length;
  $('snapskipped').textContent = s.skipped.length ? 'skipped: ' + s.skipped.map(x => x.table + ' (' + x.reason + ')').join(', ') : '';
}

// ---- Behind source (#150) ----
// Everything is derived on the server from one sample a second: the rates and state over the last minute, the backlog
// line over the last 5 min. The page only formats it.
const BS_STATE = {
  caught_up: { text: 'caught up', pill: 'border-green/40 bg-green/15 text-green', dot: 'bg-green', line: '#32a467', sub: 'backlog within the threshold' },
  catching_up: { text: 'catching up', pill: 'border-blue/40 bg-blue/15 text-blue', dot: 'bg-blue', line: '#4c90f0', sub: 'delivery is faster than the source' },
  falling_behind: { text: 'falling behind', pill: 'border-amber/40 bg-amber/15 text-amber', dot: 'bg-amber', line: '#ec9a3c', sub: 'the backlog does not shrink' },
  stalled: { text: 'stalled', pill: 'border-red/40 bg-red/15 text-red', dot: 'bg-red', line: '#e76a6e', sub: 'nothing delivered for 30 s' },
  unknown: { text: 'unknown', pill: 'border-ink-500 bg-ink-700 text-ink-200', dot: 'bg-ink-300', line: '#738091', sub: 'waiting for the source' },
};
const PILL = 'inline-flex items-center gap-2 rounded-sm border px-3 py-1 font-mono text-lg font-semibold uppercase tracking-wider ';
const secs = v => v == null ? '–' : v < 1 ? (v > 0 ? '< 1s' : '0s') : dur(Math.round(v));
function backlogSpark(h, color) {
  if (!h || h.length < 2) { $('bsspark').innerHTML = ''; return; }
  const max = Math.max(1, ...h), n = h.length;
  const pts = h.map((v, i) => (120 * i / (n - 1)).toFixed(1) + ',' + (22 - 20 * v / max).toFixed(1)).join(' ');
  $('bsspark').innerHTML = `<polyline points="${pts}" fill="none" stroke="${color}" stroke-width="1.5" vector-effect="non-scaling-stroke"/>`;
}
function renderBehind(d) {
  const l = d.source_lag || { state: 'unknown', threshold: {} };
  const st = BS_STATE[l.state] || BS_STATE.unknown;
  const pg = l.rate_unit === 'bytes/s';
  $('bsstate').className = PILL + st.pill;
  $('bsdot').className = 'h-2 w-2 rounded-full ' + st.dot;
  $('bsstatetext').textContent = st.text;
  $('bsstatesub').textContent = l.state === 'unknown'
    ? (l.head_age_secs != null && l.head_age_secs > 3 ? 'source head not read for ' + secs(l.head_age_secs)
      : l.backlog_bytes != null || l.behind_secs != null ? 'a backlog; rates need 2 s of samples' : st.sub)
    : st.sub + (l.state !== 'caught_up' ? '' : pg ? ' (≤ ' + bytes(l.threshold.backlog_bytes) + ')' : ' (≤ ' + l.threshold.behind_secs + ' s)');
  $('bshead').textContent = d.source_head_lsn ? 'WAL head ' + d.source_head_lsn + ' · ' + Math.round(l.window_secs || 0) + ' s window'
    : d.source_head_time_ms ? 'cluster time ' + isoMs(d.source_head_time_ms) + ' · ' + Math.round(l.window_secs || 0) + ' s window' : 'no head reported yet';
  $('bshead').title = $('bshead').textContent;
  if (pg) {
    $('bsbacklog').textContent = l.backlog_bytes == null ? '–' : bytes(l.backlog_bytes);
    $('bsbehind').textContent = l.behind_secs == null ? 'time behind –' : secs(l.behind_secs) + ' behind';
  } else {
    $('bsbacklog').textContent = l.behind_secs == null ? '–' : secs(l.behind_secs);
    $('bsbehind').textContent = l.behind_secs == null ? '–' : 'of cluster time';
  }
  backlogSpark(l.history, st.line);
  const rate = v => v == null ? '–' : pg ? bytes(Math.round(v)) + '/s' : v.toFixed(2) + ' s/s';
  $('bsin').textContent = rate(l.ingest_rate);
  $('bsout').textContent = rate(l.process_rate);
  const top = Math.max(l.ingest_rate || 0, l.process_rate || 0);
  $('bsinbar').style.width = (top ? 100 * (l.ingest_rate || 0) / top : 0) + '%';
  $('bsoutbar').style.width = (top ? 100 * (l.process_rate || 0) / top : 0) + '%';
  const net = l.ingest_rate == null ? null : l.process_rate - l.ingest_rate;
  $('bsnet').textContent = net == null ? 'rates need 2 s of samples'
    : Math.abs(net) < (pg ? 1 : 0.005) ? 'steady' : (net > 0 ? 'shrinking ' : 'growing ') + rate(Math.abs(net));
  $('bseta').textContent = l.state === 'caught_up' ? '0s' : l.eta_secs == null ? '–' : secs(l.eta_secs);
  $('bseta').className = TILE + (l.state === 'stalled' ? 'text-red' : l.state === 'falling_behind' ? 'text-amber' : 'text-ink-100');
  $('bsetasub').textContent = l.state === 'caught_up' ? 'nothing to catch up'
    : l.state === 'stalled' ? 'no progress' : l.eta_secs == null ? (l.state === 'unknown' ? 'at the current rates' : 'not shrinking') : 'at the current rates';
  const s = l.snapshot;
  $('bssnap').hidden = !s;
  if (s) {
    $('bssnaprate').textContent = s.rows_per_sec == null ? '–' : fmt(Math.round(s.rows_per_sec));
    $('bssnapleft').textContent = s.rows_left == null ? '–' : fmt(s.rows_left);
    $('bssnapleft').title = s.rows_left == null ? 'no planner estimate for a table (run ANALYZE)' : 'planner estimate of all tables minus rows copied by this process';
    $('bssnapeta').textContent = s.rows_left === 0 ? 'estimate exceeded' : s.eta_secs == null ? '–' : secs(s.eta_secs);
    $('bsthrottled').hidden = !s.throttled;
    $('bsthrottled').textContent = 'throttled by snapshot_max_rows_per_sec' + (s.max_rows_per_sec ? ' = ' + fmt(s.max_rows_per_sec) : '');
    $('bssnaplive').textContent = st.text;
    $('bssnaplive').className = 'font-medium ' + st.pill.split(' ').filter(c => c.startsWith('text-')).join(' ');
  }
}

// ---- The top tiles' lines: the last 300 s, one sample per second, the same window as the routing cards, kept by the
// server (/api/kpis/history) and sent with every stream frame (`kpis`). Drawn like the routing cards' lines (spark):
// a step line from 0 over the whole window, right-aligned, so a younger process draws only its right part. A second
// without a value (lag while idle, latency with nothing delivered) is drawn as 0.
const KPI = {
  throughput: { name: 'Throughput', f: v => fmt(Math.round(v)) + ' evt/s' },
  kafka_mb_per_sec: { name: 'Kafka bandwidth', f: v => mbps(v) },
  lag_ms: { name: 'End-to-end lag', f: ms },
  sink_ms: { name: 'Sink latency', f: ms },
  errors: { name: 'Errors', f: v => fmt(Math.round(v)) },
};
// MB (10^6 bytes) per second: two decimals below 10, one below 100, none above.
const mb = v => v < 10 ? v.toFixed(2) : v < 100 ? v.toFixed(1) : fmt(Math.round(v));
const mbps = v => mb(v) + ' MB/s';
function drawKpi(el, k, w) {
  const h = w[k] || [], c = KPI[k], svg = el.firstElementChild, vals = h.filter(v => v != null);
  if (!vals.length) { svg.innerHTML = ''; el.title = c.name + ': no values yet'; return; }
  const max = Math.max(...vals), min = Math.min(...vals);
  svg.innerHTML = spark([[h.map(v => v ?? 0), TABLE_LINE, 1.5]], w.len);
  const now = h[h.length - 1];
  el.title = `${c.name}, last ${Math.round(w.len * w.interval_ms / 1000)} s\nnow ${now == null ? '–' : c.f(now)} · low ${c.f(min)} · high ${c.f(max)}`;
}
// An older server sends no `kpis`: the tiles keep their numbers, without lines.
function drawKpis(w) {
  if (!w) return;
  for (const el of document.querySelectorAll('[data-kpi]')) drawKpi(el, el.dataset.kpi, w);
}

function render(d) {
  $('state-text').textContent = d.state;
  $('state-dot').className = DOT_BASE + (STATE_DOT[d.state] || 'bg-ink-300');
  $('uptime').textContent = dur(d.uptime_secs);

  // Opened from a topic's detail panel only; closed for good when the server turns it off.
  if (!d.inspector && !$('inspector').hidden) closeInspector();
  const sm = smooth(d.history);
  $('rate').textContent = fmt(Math.round(sm.length ? sm[sm.length - 1] : d.events_per_sec));
  $('rate-now').textContent = fmt(d.events_per_sec);
  $('peak').textContent = fmt(Math.round(Math.max(0, ...sm)));
  // Bandwidth: bytes Kafka acknowledged, smoothed over the same window as the throughput.
  const bw = ((d.kpis && d.kpis.kafka_mb_per_sec) || []).slice(-SMOOTH_WINDOW);
  $('kbw').textContent = mb(avg(bw));
  $('kbw-now').textContent = mb(bw.length ? bw[bw.length - 1] : 0);
  $('kbw-total').textContent = fmt(Math.round((d.kafka_bytes_total || 0) / 1e6));
  drawKpis(d.kpis);
  if (lastTotal !== null && d.events_total > lastTotal) {
    sinkSamples.push(d.sink_last_ms);
    if (sinkSamples.length > SMOOTH_WINDOW) sinkSamples.shift();
  }
  lastTotal = d.events_total;
  // Caught up with nothing delivered for a while: the lag would only count the idle time up.
  const caughtUp = d.source_lag && d.source_lag.state === 'caught_up';
  const idle = caughtUp && (d.last_event_unix_ms == null || Date.now() - d.last_event_unix_ms > IDLE_AFTER_MS);
  $('lag').textContent = idle ? 'idle' : d.lag_ms == null ? '–' : ms(Math.max(0, d.lag_ms));
  $('lagsub').textContent = idle ? 'caught up · no new changes' : caughtUp ? 'commit → delivered · caught up' : 'commit → delivered';
  $('sinkavg').textContent = d.events_total ? ms(d.sink_avg_ms) : '–';
  $('sinkdet').textContent = d.events_total ? `${sinkSamples.length ? 'recent ' + ms(avg(sinkSamples)) : 'last ' + ms(d.sink_last_ms)} · max ${ms(d.sink_max_ms)}` : 'mean per event';
  $('errors').textContent = fmt(d.errors);
  $('errors').className = 'mt-2 font-mono text-3xl font-medium tabular-nums ' + (d.errors ? 'text-red' : 'text-ink-100');
  $('errsub').textContent = d.errors ? 'see last fault below' : 'no faults';
  renderBehind(d);
  renderSnapshot(d);
  lastData = d;

  fillTableMarks();
  appendRates(d.rates);
  renderGraph(true);

  $('errpanel').hidden = !d.last_error;
  if (d.last_error) $('lasterr').textContent = d.last_error;
}

// ---- Per-table high-water marks (shown in a table's detail panel in the routing graph) ----
// Pipeline numbers (counts, last LSN, commit and delivery times) come with every stream frame;
// each table's topic comes from /api/tables, which the server caches for 5 s. The routing graph
// takes each table's topic from it; the page does not show its Kafka offsets.
const HWM_POLL_MS = 5000;
// Last-commit tone, only while streaming: a table whose newest change is this far behind the
// freshest table's turns amber / red. Measured against the freshest table, not the clock, so a
// database that is idle as a whole is not flagged.
const BEHIND_AMBER_MS = 60000, BEHIND_RED_MS = 600000;
let hwm = { tables: {} };
const ago = t => { const a = Math.max(0, Date.now() - t); return a < 1000 ? 'now' : dur(Math.floor(a / 1000)) + ' ago'; };

async function pollTables() {
  try {
    const r = await fetch('/api/tables', authInit());
    if (r.status === 401 || r.status === 403) { clearInterval(tablesTimer); return; }
    if (!r.ok) return;
    const d = await r.json();
    const byName = {};
    for (const t of d.tables) byName[t.table] = t;
    hwm = { tables: byName };
    renderGraph();
    fillTableMarks();
  } catch (_) { /* the connection badge covers outages */ }
}
const tablesTimer = setInterval(pollTables, HWM_POLL_MS);
pollTables();

function setConn(live, text) {
  $('conn-dot').className = DOT_BASE + (live ? 'bg-green' : 'bg-red');
  $('conn-text').textContent = text;
}

// ---- Event inspector ----
const OP_BADGE = {
  insert: 'bg-green/15 text-green', update: 'bg-blue/15 text-blue',
  delete: 'bg-red/15 text-red', truncate: 'bg-violet/15 text-violet',
  read: 'bg-amber/15 text-amber',
};
// The inspector shows a sample, not a feed: the newest SAMPLE_EVENTS records, read once when it opens, when the topic
// filter changes, or on Resample.
const SAMPLE_EVENTS = 100;
let evs = [];          // newest first, as read from Kafka
let sampledAt = null;  // when the sample was read (browser clock)
let sampling = false;
let openId = null;

const evid = e => e.topic + '/' + e.partition + '/' + e.offset;
// Events use Debezium's envelope: op letters, and the table and position in `source`. A value that is not an
// envelope (an outbox payload, a `format = "row"` row, a tombstone) is shown raw, with its op and lsn from the
// record headers.
const OP_NAME = { c: 'insert', u: 'update', d: 'delete', t: 'truncate', r: 'read' };
const isEnv = ev => !!ev && typeof ev === 'object' && !Array.isArray(ev) && !!ev.source && typeof ev.source === 'object' && typeof ev.op === 'string';
const hdr = (e, k) => (e.headers || {})[k];
const opOf = e => { const o = isEnv(e.event) ? e.event.op : hdr(e, 'op'); return Object.hasOwn(OP_NAME, o) ? OP_NAME[o] : o; };
const opBadge = e => { const o = opOf(e); return Object.hasOwn(OP_BADGE, o) ? OP_BADGE[o] : ''; };
const src = ev => isEnv(ev) ? ev.source : {};
const lsnOf = e => isEnv(e.event) ? src(e.event).lsn ?? '' : hdr(e, 'lsn') ?? '';
const rawText = v => typeof v === 'string' ? v : JSON.stringify(v, null, 2) ?? '';
// The table of an envelope. A row-format value or a tombstone names it in its JSON record key ({db, schema, table}
// or {db, collection}); an outbox payload names no table, so its topic stands in.
const keyRel = e => { try { const k = JSON.parse(e.key); return k && typeof k === 'object' && typeof k.db === 'string' && (k.table != null || k.collection != null) ? (k.schema ?? k.db) + '.' + (k.table ?? k.collection) : null; } catch (_) { return null; } };
const rel = e => !isEnv(e.event) ? keyRel(e) || e.topic :(src(e.event).schema || src(e.event).db || '?') + '.' + (src(e.event).table || src(e.event).collection || '?');
const isoMs = ms => { const d = new Date(ms); return isNaN(d) ? '–' : d.toISOString(); };
const clock = ms => new Date(ms).toLocaleTimeString('en-GB', { hour12: false }) + '.' + String(ms % 1000).padStart(3, '0');
const short = v => { const t = typeof v === 'string' ? v : JSON.stringify(v) ?? ''; return t.length > 28 ? t.slice(0, 27) + '…' : t; };
const summary = e => {
  const ev = e.event;
  if (ev === null) return '<span class="text-ink-500">tombstone</span>';
  if (!isEnv(ev)) return `<span class="text-ink-500">raw</span> ${esc(short(ev))}`;
  const row = ev.after || ev.before;
  return row ? Object.entries(row).slice(0, 4).map(([k, v]) => `<span class="text-ink-300">${esc(k)}=</span>${esc(short(v))}`).join(' ') : '<span class="text-ink-500">–</span>';
};

function side(title, row, other) {
  const head = `<div class="mb-1 text-[11px] uppercase tracking-widest text-ink-300">${title}</div>`;
  if (!row) return `<div>${head}<div class="text-ink-500">–</div></div>`;
  const lines = Object.entries(row).map(([k, v]) => {
    const changed = other && JSON.stringify(other[k]) !== JSON.stringify(v);
    return `<div class="${changed ? 'bg-amber/10 text-amber' : ''} px-1.5"><span class="text-ink-300">${esc(k)}</span>: ${esc(JSON.stringify(v))}</div>`;
  }).join('');
  return `<div>${head}${lines}</div>`;
}

function detail(e) {
  const ev = e.event;
  if (!isEnv(ev)) {
    const heads = Object.entries(e.headers || {}).map(([k, v]) => `<span>${esc(k)} ${esc(v)}</span>`).join('');
    return `<tr class="border-b border-ink-700 bg-ink-900"><td colspan="6" class="px-4 py-3"><div class="mb-3 flex items-center gap-4 text-[11px] text-ink-300"><span>key ${esc(e.key || '–')}</span>${heads}<button type="button" data-copy="${esc(evid(e))}" class="ml-auto rounded-sm border border-ink-600 bg-ink-700 px-2 py-0.5 uppercase tracking-wider text-ink-100 hover:border-blue">Copy JSON</button></div><div class="mb-1 text-[11px] uppercase tracking-widest text-ink-300">Value (not an envelope)</div><div class="whitespace-pre-wrap break-words px-1.5">${esc(rawText(ev))}</div></td></tr>`;
  }
  const isUpd = opOf(e) === 'update';
  return `<tr class="border-b border-ink-700 bg-ink-900"><td colspan="6" class="px-4 py-3"><div class="mb-3 flex items-center gap-4 text-[11px] text-ink-300"><span>key ${esc(e.key || '–')}</span><span>xid ${esc(src(ev).txId ?? '–')}</span><span>commit ${esc(isoMs(src(ev).ts_ms))}</span><button type="button" data-copy="${esc(evid(e))}" class="ml-auto rounded-sm border border-ink-600 bg-ink-700 px-2 py-0.5 uppercase tracking-wider text-ink-100 hover:border-blue">Copy JSON</button></div><div class="grid gap-6 md:grid-cols-2">${side('Before', ev.before, isUpd ? ev.after : null)}${side('After', ev.after, isUpd ? ev.before : null)}</div></td></tr>`;
}

function renderEvents() {
  const q = $('evsearch').value.trim().toLowerCase();
  const op = $('evop').value, tb = $('evtable').value;
  const rows = evs.filter(e => (!op || opOf(e) === op) && (!tb || rel(e) === tb) && (!q || JSON.stringify(e.event).toLowerCase().includes(q)));
  const when = sampledAt ? ' · sampled ' + new Date(sampledAt).toLocaleTimeString() : '';
  $('evcount').textContent = (rows.length === evs.length ? fmt(evs.length) + ' events' : rows.length + ' of ' + evs.length) + when;
  $('events').innerHTML = rows.length ? rows.slice(0, 200).map(e => {
    const ev = e.event, id = evid(e);
    const row = `<tr data-id="${esc(id)}" class="cursor-pointer border-b border-ink-700 hover:bg-ink-700/50 ${openId === id ? 'bg-ink-700/50' : ''}"><td class="px-4 py-1.5 tabular-nums text-ink-300">${esc(e.topic)} <span class="text-ink-500">p${esc(e.partition)} @${esc(e.offset)}</span></td><td class="px-4 py-1.5 tabular-nums text-ink-200">${esc(clock(e.timestamp_ms))}</td><td class="px-4 py-1.5"><span class="rounded-sm px-1.5 py-0.5 text-[11px] uppercase ${opBadge(e)}">${esc(opOf(e) || '?')}</span></td><td class="px-4 py-1.5 text-ink-100">${esc(rel(e))}</td><td class="px-4 py-1.5 text-ink-300">${esc(String(lsnOf(e)))}</td><td class="px-4 py-1.5 text-ink-200">${summary(e)}</td></tr>`;
    return openId === id ? row + detail(e) : row;
  }).join('') : `<tr><td colspan="6" class="px-4 py-6 text-center text-ink-300">${evs.length ? 'No events match the filters' : 'No events in Kafka yet'}</td></tr>`;
}

function syncTableFilter() {
  const sel = $('evtable'), cur = sel.value;
  const names = [...new Set(evs.map(rel))].sort();
  if (names.join() === [...sel.options].slice(1).map(o => o.value).join()) return;
  sel.innerHTML = '<option value="">All tables</option>' + names.map(n => `<option>${esc(n)}</option>`).join('');
  sel.value = cur;
}

function syncTopicFilter(topics) {
  const sel = $('evtopic'), cur = sel.value;
  if (topics.join() === [...sel.options].slice(1).map(o => o.value).join()) return;
  sel.innerHTML = '<option value="">All topics</option>' + topics.map(n => `<option>${esc(n)}</option>`).join('');
  sel.value = cur;
}

async function sampleEvents() {
  // Closed inspector: no Kafka reads at all.
  if (sampling || $('inspector').hidden) return;
  sampling = true;
  $('evresample').disabled = true;
  $('evresample').textContent = 'Sampling…';
  try {
    const r = await fetch('/api/events?limit=' + SAMPLE_EVENTS + '&topic=' + encodeURIComponent($('evtopic').value), authInit());
    if (r.status === 403 || r.status === 401) {
      $('events').innerHTML = `<tr><td colspan="6" class="px-4 py-6 text-center text-ink-300">${r.status === 403 ? 'Inspector disabled. Set admin_token to enable it (it exposes row data).' : 'Unauthorized. Open the page once with #token=... (it is moved to session storage and removed from the URL).'}</td></tr>`;
      return;
    }
    if (!r.ok) return;
    const d = await r.json();
    evs = d.events;
    sampledAt = Date.now();
    openId = null;
    syncTopicFilter(d.topics);
    syncTableFilter();
    renderEvents();
  } catch (_) { /* the connection badge covers outages */ } finally {
    sampling = false;
    $('evresample').disabled = false;
    $('evresample').textContent = 'Resample';
  }
}

$('events').addEventListener('click', ev => {
  const copy = ev.target.closest('[data-copy]');
  if (copy) {
    const e = evs.find(x => evid(x) === copy.dataset.copy);
    if (e && navigator.clipboard) navigator.clipboard.writeText(typeof e.event === 'string' ? e.event : JSON.stringify(e.event, null, 2));
    return;
  }
  const tr = ev.target.closest('tr[data-id]');
  if (!tr) return;
  openId = openId === tr.dataset.id ? null : tr.dataset.id;
  renderEvents();
});
$('snapcancel').addEventListener('click', async () => {
  if (!confirm('Cancel the snapshot after the chunk in flight?')) return;
  $('snapcancel').disabled = true;
  try {
    const r = await fetch('/api/snapshot', authInit({ method: 'DELETE' }));
    if (!r.ok) alert('Cancel failed: ' + ((await r.json().catch(() => ({}))).error || r.status));
  } finally {
    $('snapcancel').disabled = false;
  }
});
for (const id of ['evsearch', 'evop', 'evtable']) $(id).addEventListener('input', renderEvents);
$('evtopic').addEventListener('input', sampleEvents);
$('evresample').addEventListener('click', sampleEvents);
// The inspector is a modal pop-up: Close, Esc or a click outside the window closes it.
function closeInspector() {
  $('inspector').hidden = true;
  document.body.classList.remove('overflow-hidden');
}
$('evclose').addEventListener('click', closeInspector);
$('inspector').addEventListener('click', ev => { if (ev.target === $('inspector')) closeInspector(); });
document.addEventListener('keydown', ev => { if (ev.key === 'Escape' && !$('inspector').hidden) closeInspector(); });

// ---- Routing graph ----
// Tables (stream + /api/tables) → routes in match order, first match wins → topics, and the dead-letter topic.
// The plan is read once from /api/routes; counts and unseen fields come with every stream frame. Nodes are
// HTML, edges an SVG drawn behind them from the nodes' measured positions.
const GRAPH_TABLES = 6, GRAPH_TOPICS = 6, FLOW_MS = 3000;
const EDGE_BASE = '#404854', EDGE_HL = '#4c90f0', EDGE_FLOW = '#abb3bf', EDGE_RED = '#e76a6e';
// Outcome counters of a route, in the order of the stream's `rates.routes` rows.
const ROUTE_COUNTS = ['produced', 'filtered', 'fallback', 'errors'];
// A route's other outcomes per op and second, in the order of a stream frame's `route_outcome_ops` rows.
const ROUTE_OUTCOME_OPS = ['filtered_ops', 'fallback_ops', 'errors_ops'];
// Detail-chart line colours: a route's produced line; the dead-letter topic in red.
const RATE_LINE = { produced: '#32a467', filtered: '#738091', fallback: '#ad7bee', errors: '#e76a6e' };
// Source (table) and sink (topic) step lines are white, so colours only mean route metrics; dead-letter lines stay red.
const TOPIC_LINE = '#e6e9ee', TABLE_LINE = '#e6e9ee';
// A route card's metrics and step lines: processed (every event it took), matched (processed minus filtered: the
// `where` held), fallback, errors.
const CARD_METRICS = [['processed', '#4c90f0'], ['passed', '#32a467', 'matched'], ['fallback', '#ad7bee'], ['errors', '#e76a6e']];

// ---- Rates per route and per topic ----
// /api/routes/history gives the last 300 s at 1 s (route outcomes and produced records per op, records Kafka
// acknowledged per topic and per op, and each topic's per-op totals since start); each stream frame then carries
// only the newest second (`rates`), numbered by `seq`, which is also added to the topic totals. An older server
// sends no per-op data: the route and topic lines then count every op, and their op bars stay empty. A frame that does
// not follow the window (a reconnect, a missed second) reloads it. Nothing here animates. A table's events
// per second come with every frame instead (`history_ops`: the same 300 s at 1 s, sampled in the same tick).
let rates = null, ratesBusy = false, ratesOff = false;
async function loadRates() {
  if (ratesBusy || ratesOff) return;
  ratesBusy = true;
  try {
    const r = await fetch('/api/routes/history', authInit());
    if (r.status === 401 || r.status === 403 || r.status === 404) ratesOff = true;
    else if (r.ok) rates = await r.json();
  } catch (_) { /* retried on the next frame */ }
  ratesBusy = false;
  renderGraph();
}
function appendRates(f) {
  if (!f || ratesOff) return;
  if (!rates) { loadRates(); return; }
  if (f.seq === rates.seq) return;
  if (f.seq !== rates.seq + 1 || f.routes.length !== rates.routes.length) { loadRates(); return; }
  const filled = Math.min(f.seq, rates.len);
  const add = (a, v) => { a.push(v); while (a.length > rates.len) a.shift(); };
  rates.routes.forEach((r, i) => {
    ROUTE_COUNTS.forEach((k, j) => add(r[k], f.routes[i][j]));
    if (r.ops && f.route_ops) add(r.ops, f.route_ops[i]);
    if (r.errors_ops && f.route_outcome_ops) ROUTE_OUTCOME_OPS.forEach((k, j) => add(r[k], f.route_outcome_ops[i][j]));
  });
  for (const t of Object.keys(f.topics)) if (!rates.topics[t]) rates.topics[t] = new Array(filled - 1).fill(0);
  for (const [t, a] of Object.entries(rates.topics)) add(a, f.topics[t] || 0);
  if (rates.topic_ops && rates.topic_totals && f.topic_ops) {
    for (const t of Object.keys(f.topic_ops)) if (!rates.topic_ops[t]) rates.topic_ops[t] = Array.from({ length: filled - 1 }, () => NO_OPS);
    for (const [t, a] of Object.entries(rates.topic_ops)) add(a, f.topic_ops[t] || NO_OPS);
    for (const [t, o] of Object.entries(f.topic_ops)) {
      const tot = rates.topic_totals[t] || (rates.topic_totals[t] = {});
      OP_LABEL.forEach((k, j) => { tot[k] = (tot[k] || 0) + (o[j] || 0); });
    }
  }
  if (rates.topic_bytes && f.topic_bytes) {
    for (const t of Object.keys(f.topic_bytes)) if (!rates.topic_bytes[t]) rates.topic_bytes[t] = new Array(filled - 1).fill(0);
    for (const [t, a] of Object.entries(rates.topic_bytes)) add(a, f.topic_bytes[t] || 0);
  }
  rates.seq = f.seq;
}
// A route's window by outcome, if the history matches the plan; a topic's acknowledged records per second.
const routeRate = i => rates && plan && rates.routes[i] && rates.routes[i].route === plan.routes[i].route ? rates.routes[i] : null;
const topicRate = name => (rates && rates.topics[name]) || [];
// Pathway bandwidth. The sink counts bytes per topic (`topic_bytes`; an older server sends none), not per route or
// table, so a pathway's bandwidth is its records per second (mean of the last SMOOTH_WINDOW s) times the bytes per
// record its topics averaged over the window. A table's pathway carries its share (by events) of what its route
// produces.
const bwKnown = () => !!(rates && rates.topic_bytes);
const recent = h => avg((h || []).slice(-SMOOTH_WINDOW));
// A card's rate: the mean of the last SMOOTH_WINDOW s, per second.
const perSec = v => (v >= 100 ? Math.round(v) : v >= 10 ? v.toFixed(0) : v.toFixed(1)) + '/s';
const total = h => (h || []).reduce((a, n) => a + n, 0);
const bytesPerRecord = names => {
  if (!bwKnown()) return 0;
  let b = 0, n = 0;
  for (const t of names) { b += total(rates.topic_bytes[t]); n += total(rates.topics[t]); }
  return n ? b / n : 0;
};
const bwText = b => b >= 1e5 ? mbps(b / 1e6) : b >= 1e3 ? (b / 1e3).toFixed(1) + ' kB/s' : Math.round(b) + ' B/s';
// Per-op rows and totals are [insert, update, delete, truncate, read], the keys of the server's op counts.
const OP_LABEL = ['insert', 'update', 'delete', 'truncate', 'read'];
const NO_OPS = [0, 0, 0, 0, 0];
// {insert, update, ...} as a row; null when the server sent none.
const opSlots = o => o ? OP_LABEL.map(k => o[k] || 0) : null;
// The sum of rows; null when none is known.
const addSlots = list => list.some(Boolean) ? list.reduce((a, r) => r ? a.map((v, i) => v + r[i]) : a, NO_OPS) : null;
// A topic's records acknowledged per op since start; null without per-op data from the server.
const topicTotals = name => rates && rates.topic_totals ? opSlots(rates.topic_totals[name]) || NO_OPS : null;
// Ops the detail chart's single line counts. One selection for the table, route and topic panels, kept while you
// click along a path (table → its route → its topic), so the lines you compare count the same ops.
const detailOps = new Set([0, 1, 2, 3, 4]);
// Toggle buttons that pick those ops.
const OP_BTN = 'rounded-sm border px-2 py-0.5 uppercase tracking-wider ';
const OP_BTN_OFF = OP_BTN + 'border-ink-600 bg-ink-700 text-ink-500';
// Op buttons for the ops that have happened (totals per op, insert..read); without totals (an older server) all five.
const opMask = totals => totals ? totals.map(n => n > 0 ? 1 : 0).join('') : '11111';
// A toggle button: when on, tinted in its line's colour (it is the chart's legend); when off, grey.
const toggleAttrs = (color, on) => on ? `class="${OP_BTN}" style="border-color:${color};color:${color};background-color:${color}26"` : `class="${OP_BTN_OFF}"`;
const opBtnAttrs = (op, on) => toggleAttrs(OPLINE[op], on);
// The outcome the route panel charts (a CARD_METRICS index), matched by default; kept while you click from route to
// route. Picked from a dropdown.
let detailLine = 1;
const lineSelect = () => `<select id="detailline" class="rounded-sm border border-ink-600 bg-ink-900 px-2 py-0.5 uppercase tracking-wider text-ink-100 focus:border-blue focus:outline-none" title="Outcome to chart">`
  + CARD_METRICS.map(([k, , label], i) => `<option value="${i}"${i === detailLine ? ' selected' : ''}>${label || k}</option>`).join('') + '</select>';
const opButtons = totals => `<span id="detailopbtns" data-mask="${opMask(totals)}" class="contents">`
  + OP_LABEL.map((label, op) => totals && !(totals[op] > 0) ? ''
    : `<button type="button" data-op="${op}" aria-pressed="${detailOps.has(op)}" ${opBtnAttrs(op, detailOps.has(op))}>${label}</button>`).join('')
  + '</span>';
// The ops a route has taken: produced since start plus its other outcomes over the window (the server counts those
// per op only for the rate history); null without per-op counts (an older server).
const routeOpTotals = (x, rr) => {
  const t = opSlots(x.c.ops);
  if (!t || !rr || !rr.errors_ops) return null;
  return addSlots([t, ...ROUTE_OUTCOME_OPS.map(k => addSlots(rr[k] || []))]);
};
// Sum of series, aligned on their newest sample.
const sumSeries = list => {
  const n = Math.max(0, ...list.map(a => a.length));
  return Array.from({ length: n }, (_, i) => list.reduce((s, a) => s + (a[i - (n - a.length)] || 0), 0));
};
const rateStats = h => {
  const max = Math.max(0, ...h);
  return `now ${fmt(h.length ? h[h.length - 1] : 0)}/s · avg ${avg(h).toFixed(1)}/s · max ${fmt(max)}/s`;
};
// A card's step line over the last n samples, 300 s by default (right-aligned, so a short window starts mid-way).
// lines: [[series, colour, width]], scaled together.
const SPARK_N = 300;
function spark(lines, n = SPARK_N) {
  const W = 120, H = 20, max = Math.max(1, ...lines.flatMap(l => l[0]));
  const y = v => H - 1 - (H - 2) * v / max;
  return lines.map(([h, c, w]) => {
    if (h.length < 2) return '';
    const off = n - h.length, x = i => W * (i + off) / (n - 1);
    return `<polyline points="${stepPts(h, x, y)}" fill="none" stroke="${c}" stroke-width="${w}" vector-effect="non-scaling-stroke"/>`;
  }).join('');
}
const sparkSvg = (id, cls, style) => `<svg data-spark="${esc(id)}" viewBox="0 0 120 20" preserveAspectRatio="none" class="${cls}"${style ? ` style="${style}"` : ''} aria-hidden="true"></svg>`;
// A table or topic card's step line, under its name and count.
const nodeLine = id => `<div class="mt-1">${sparkSvg(id, 'block w-full', 'height:36px')}</div>`;
// A table's events per second, all ops.
const tableEventsRate = name => ((lastData && lastData.history_ops) || {})[name]?.map(o => o.reduce((a, n) => a + n, 0)) || [];
// A card's op shares since start: a stacked strip down the card's left edge (the legend's colours, top to bottom in
// OP_LABEL order), filled by fillOpStrip on every frame. A card with a strip is `relative` and padded past it.
const STRIP_CARD = 'relative overflow-hidden', STRIP_PAD = 'padding-left:15px';
// Shares are split by a 1 px gap in the card's own background, so two neighbours never blend into one.
const opStrip = id => `<span data-opstrip="${esc(id)}" class="absolute top-0 flex flex-col overflow-hidden" style="left:0;bottom:0;width:6px;gap:1px"></span>`;
function fillOpStrip(el, row, what) {
  const total = row ? row.reduce((a, n) => a + n, 0) : 0;
  const html = total ? row.map((n, i) => n ? `<span class="block" style="flex:${n} 1 0;min-height:2px;background:${OPLINE[i]}"></span>` : '').join('')
    : '<span class="block h-full w-full bg-ink-700"></span>';
  if (el.dataset.k !== html) { el.dataset.k = html; el.innerHTML = html; }
  el.title = !row ? 'no per-operation counts from this server'
    : !total ? 'no ' + what + ' yet'
    : what + ' by operation: ' + OP_LABEL.map((k, i) => `${k} ${fmt(row[i])} (${(100 * row[i] / total).toFixed(1)}%)`).join(' · ');
}

// Same as kafka.rs::glob_match: `*` matches any run of characters, dots included; nothing else is special.
// Checked on load against #globcases, which admin.rs also runs through the Rust function.
function globMatch(p, t) {
  const parts = p.split('*');
  if (parts.length === 1) return p === t;
  const first = parts[0], last = parts[parts.length - 1];
  if (!t.startsWith(first) || t.length < first.length + last.length || !t.endsWith(last)) return false;
  let rest = t.slice(first.length, t.length - last.length);
  for (const mid of parts.slice(1, -1)) {
    const i = rest.indexOf(mid);
    if (i < 0) return false;
    rest = rest.slice(i + mid.length);
  }
  return true;
}
for (const [p, t, want] of JSON.parse($('globcases').textContent)) console.assert(globMatch(p, t) === want, 'globMatch', p, t, want);

const fillTopic = (tpl, name) => { const i = name.indexOf('.'); return tpl.replaceAll('{schema}', name.slice(0, i)).replaceAll('{table}', name.slice(i + 1)); };
const isTemplate = tpl => /\{(schema|table)\}/.test(tpl);
// A template that also reads the event ({op}, {row.x}): its topics are only known from what the sink produced.
const perEvent = tpl => /\{(?!schema\}|table\})[^}]*\}/.test(tpl);
const tplGlob = tpl => tpl.replace(/\{[^}]*\}/g, '*');
let plan = null, planError = null, graphModel = null, graphKey = '';
let graphSel = null, graphHover = null, topicSel = null, tableSel = null;
const graphOpen = new Set();        // 'r:<route>' and 'g:<template>' groups shown in full
const flowUntil = {};               // edge id → time until which it counts as having recent traffic
let flowPrev = null;

// A route without `match` is the default (sink.topic), or one reached only through an earlier route's `otherwise`.
const isDefaultRoute = r => r.match == null && r.route === 'default';
const isOtherwiseRoute = r => r.match == null && r.route !== 'default';

// The graph as nodes, edges and the numbers that change with every frame.
function graphModelOf(d) {
  const live = (d && d.routes) || [];
  const routes = plan.routes.map((r, i) => {
    const l = live[i] && live[i].route === r.route ? live[i] : null;
    const c = l || Object.assign({ unseen_fields: r.unseen_fields || [] }, r.counts);
    return { r, i, c, id: 'r:' + i, tables: [], topics: new Map(), out: [] };
  });
  const fallback = routes.findIndex(x => isDefaultRoute(x.r));
  // `otherwise` chains: x.next is the route x passes the rows its where rejects on to, x.from the routes that pass to x.
  for (const x of routes) x.from = [];
  for (const x of routes) {
    x.next = x.r.otherwise != null ? routes.find(y => y.r.route === x.r.otherwise) || null : null;
    if (x.next) x.next.from.push(x);
  }
  const claim = name => { const i = routes.findIndex(x => x.r.match != null && globMatch(x.r.match, name)); return i >= 0 ? i : fallback; };
  const evs = (d && d.tables) || {};
  const unrouted = [];
  for (const name of [...new Set([...Object.keys(evs), ...Object.keys(hwm.tables)])].sort()) {
    const k = hwm.tables[name] || {}, ri = claim(name);
    const t = { name, events: evs[name] ?? k.events ?? 0, id: 't:' + name };
    if (ri < 0) { unrouted.push(t); continue; }
    const x = routes[ri];
    t.route = ri;
    x.tables.push(t);
    if (perEvent(x.r.topic)) continue;
    t.topic = k.topic || fillTopic(x.r.topic, name);
    x.topics.set(t.topic, (x.topics.get(t.topic) || 0) + t.events);
  }
  // Per-event templates: the topics the sink acknowledged records on (and the fallback), weighted by the window.
  // Dead-letter topics are drawn on their own, so a template's glob does not claim them (`orders.*`, `orders.unknown.dlq`).
  const dlqNames = [...new Set(plan.routes.map(dlqOf).filter(Boolean))];
  const isDlqTopic = t => dlqNames.some(d => d === t || (d.includes('{') && globMatch(tplGlob(d), t)));
  for (const x of routes.filter(x => perEvent(x.r.topic) && rates)) {
    const glob = tplGlob(x.r.topic);
    for (const [t, h] of Object.entries(rates.topics)) {
      if ((globMatch(glob, t) && !isDlqTopic(t)) || t === x.r.topic_fallback) x.topics.set(t, h.reduce((a, n) => a + n, 0));
    }
  }
  // Tables per route: the busiest few, the rest behind "+N more".
  // ops: each card's counts per op ([insert, update, delete, truncate, read]) for its op bar.
  const values = {}, ops = {}, edges = [], tableEdges = {};
  const tableOps = name => ((d && d.table_ops) || {})[name] || NO_OPS;
  for (const x of routes) {
    const all = [...x.tables].sort((a, b) => b.events - a.events);
    const cut = graphOpen.has(x.id) || all.length <= GRAPH_TABLES ? all.length : GRAPH_TABLES - 1;
    x.shown = all.slice(0, cut).sort((a, b) => a.name < b.name ? -1 : 1);
    x.hidden = all.slice(cut);
    x.moreId = 'tm:' + x.i;
    for (const t of x.tables) { values[t.id] = fmt(t.events); values[t.id + ':rate'] = perSec(recent(tableEventsRate(t.name))); ops[t.id] = tableOps(t.name); }
    if (x.hidden.length) {
      values[x.moreId] = fmt(x.hidden.reduce((a, t) => a + t.events, 0));
      values[x.moreId + ':rate'] = perSec(x.hidden.reduce((a, t) => a + recent(tableEventsRate(t.name)), 0));
      ops[x.moreId] = addSlots(x.hidden.map(t => ops[t.id]));
    }
    ops[x.id] = opSlots(x.c.ops);
    for (const k of ['matched', ...ROUTE_COUNTS]) values[x.id + ':' + k] = fmt(x.c[k] || 0);
    values[x.id + ':processed'] = fmt(x.c.matched || 0);
    values[x.id + ':passed'] = fmt(Math.max(0, (x.c.matched || 0) - (x.c.filtered || 0)));
    // The card: its match rate (matched / processed over the rate window, since start without one) and the events per
    // second it funnels on (matched).
    const rr = routeRate(x.i);
    const proc = rr ? total(sumSeries(ROUTE_COUNTS.map(k => rr[k] || []))) : x.c.matched || 0;
    const pass = rr ? proc - total(rr.filtered) : Math.max(0, (x.c.matched || 0) - (x.c.filtered || 0));
    values[x.id + ':matchrate'] = proc ? (100 * pass / proc).toFixed(pass === proc ? 0 : 1) + '%' : '–';
    values[x.id + ':funnel'] = perSec(rr ? recent(sumSeries([rr.produced || [], rr.fallback || [], rr.errors || []])) : 0);
  }
  // Topics, grouped by the template that renders them; a topic two routes share is drawn once.
  const groups = new Map(), owner = new Map();
  const addTopic = (g, name, n) => {
    const og = owner.get(name) || g;
    owner.set(name, og);
    og.topics.set(name, (og.topics.get(name) || 0) + n);
  };
  for (const x of routes) {
    const tpl = x.r.topic;
    if (!groups.has(tpl)) groups.set(tpl, { tpl, dynamic: isTemplate(tpl) || perEvent(tpl), topics: new Map(), id: 'tpl:' + tpl });
    const g = groups.get(tpl);
    if (!g.dynamic) addTopic(g, tpl, 0);
    for (const [t, n] of x.topics) addTopic(g, g.dynamic ? t : tpl, n);
  }
  const topicNode = new Map();
  for (const g of groups.values()) {
    const all = [...g.topics].sort((a, b) => b[1] - a[1]).map(e => e[0]);
    const cut = graphOpen.has('g:' + g.tpl) || all.length <= GRAPH_TOPICS ? all.length : GRAPH_TOPICS - 1;
    g.shown = all.slice(0, cut).sort();
    g.hidden = all.slice(cut);
    g.moreId = 'pm:' + g.tpl;
    for (const t of g.shown) {
      topicNode.set(t, 'p:' + t);
      ops['p:' + t] = topicTotals(t);
      if (ops['p:' + t]) values['p:' + t] = fmt(ops['p:' + t].reduce((a, n) => a + n, 0));
      values['p:' + t + ':rate'] = perSec(recent(topicRate(t)));
    }
    for (const t of g.hidden) topicNode.set(t, g.moreId);
    if (g.hidden.length) ops[g.moreId] = rates && rates.topic_totals ? addSlots(g.hidden.map(topicTotals)) : null;
  }
  // Edges: table → claiming route, route → each topic it wrote (its produced count split by the tables' events),
  // route → its dead-letter topic: one node per distinct topic or template, the quietest behind "+N more".
  // w: records since start (the width without bandwidth data); bw: bytes per second now.
  const edge = (from, to, w, dlq, bw = 0) => { const id = from + '>' + to; edges.push({ id, from, to, w, dlq, bw }); return id; };
  const dlqs = new Map();
  for (const x of routes) {
    x.dlq = dlqOf(x.r);
    x.dlqNames = [];
    if (!x.dlq || !(canDeadLetter(x.r) || x.c.errors > 0)) { x.dlq = null; continue; }
    // A template shows the dead-letter topics it rendered (none yet: no node); a fixed name shows itself.
    x.dlqNames = x.dlq.includes('{') ? dlqTopics(x.dlq) : [x.dlq];
    for (const name of x.dlqNames) {
      if (!dlqs.has(name)) dlqs.set(name, { name, id: 'dlq:' + name, routes: [], errors: 0 });
      const d = dlqs.get(name);
      d.routes.push(x);
      d.errors += x.dlqNames.length === 1 ? (x.c.errors || 0)
        : (rates && rates.topic_totals && rates.topic_totals[name] ? Object.values(rates.topic_totals[name]).reduce((a, n) => a + n, 0) : ((rates && rates.topics[name]) || []).reduce((a, n) => a + n, 0));
    }
  }
  // A dead-letter topic that has had no events is not drawn (the route's panel still names it).
  for (const [name, d] of dlqs) if (!d.errors) dlqs.delete(name);
  const dlqAll = [...dlqs.values()].sort((a, b) => b.errors - a.errors || (a.name < b.name ? -1 : 1));
  const dlqCut = graphOpen.has('g:dlq') || dlqAll.length <= GRAPH_TOPICS ? dlqAll.length : GRAPH_TOPICS - 1;
  const dlqShown = dlqAll.slice(0, dlqCut).sort((a, b) => a.name < b.name ? -1 : 1), dlqHidden = dlqAll.slice(dlqCut);
  for (const d of dlqHidden) d.node = 'pm:dlq';
  for (const d of dlqShown) d.node = d.id;
  for (const d of dlqAll) { values[d.id] = fmt(d.errors); values[d.id + ':rate'] = perSec(recent(dlqRate(d.name))); }
  if (dlqHidden.length) {
    values['pm:dlq'] = fmt(dlqHidden.reduce((a, d) => a + d.errors, 0));
    values['pm:dlq:rate'] = perSec(dlqHidden.reduce((a, d) => a + recent(dlqRate(d.name)), 0));
  }
  for (const x of routes) {
    const g = groups.get(x.r.topic);
    const sum = [...x.topics.values()].reduce((a, n) => a + n, 0);
    const rr = routeRate(x.i), now = k => rr ? recent(rr[k]) : 0;
    const produced = now('produced'), errs = now('errors');
    // Per topic node: [records since start, bytes/s].
    const targets = new Map();
    const target = (to, t, share) => {
      const v = targets.get(to) || [0, 0];
      targets.set(to, [v[0] + (x.c.produced || 0) * share, v[1] + produced * share * bytesPerRecord([t])]);
    };
    if (!x.topics.size && !g.dynamic) target(topicNode.get(x.r.topic), x.r.topic, 1);
    for (const [t, n] of x.topics) target(topicNode.get(t), t, sum ? n / sum : 0);
    const dlqBw = errs * bytesPerRecord(x.dlqNames);
    // The bytes the route's tables become, shared out by their events.
    const outBw = [...targets.values()].reduce((a, v) => a + v[1], 0) + dlqBw;
    const evAll = x.tables.reduce((a, t) => a + t.events, 0);
    const share = list => evAll ? list.reduce((a, t) => a + t.events, 0) / evAll : 0;
    for (const t of x.shown) tableEdges[t.name] = [edge(t.id, x.id, t.events, false, outBw * share([t]))];
    if (x.hidden.length) {
      const id = edge(x.moreId, x.id, x.hidden.reduce((a, t) => a + t.events, 0), false, outBw * share(x.hidden));
      for (const t of x.hidden) tableEdges[t.name] = [id];
    }
    for (const [to, [w, bw]] of targets) x.out.push({ to, id: edge(x.id, to, w, false, bw) });
    for (const t of x.tables) if (t.topic && topicNode.get(t.topic)) tableEdges[t.name].push(x.id + '>' + topicNode.get(t.topic));
    for (const name of x.dlqNames) if (dlqs.has(name)) x.dlqEdge = edge(x.id, dlqs.get(name).node, x.c.errors || 0, true, dlqBw);
  }
  // otherwise: route → the later route it passes rejected rows on to, drawn as a bracket down the cards' left side.
  for (const x of routes) {
    if (!x.next) continue;
    x.chainEdge = x.id + '>' + x.next.id;
    edges.push({ id: x.chainEdge, from: x.id, to: x.next.id, w: x.c.passed_on || 0, bw: 0, chain: true });
    values[x.chainEdge] = fmt(x.c.passed_on || 0);
  }
  for (const t of unrouted) { values[t.id] = fmt(t.events); values[t.id + ':rate'] = perSec(recent(tableEventsRate(t.name))); ops[t.id] = tableOps(t.name); }
  for (const d of dlqAll) ops[d.id] = dlqTotals(d.name);
  if (dlqHidden.length) ops['pm:dlq'] = rates && rates.topic_totals ? addSlots(dlqHidden.map(d => ops[d.id])) : null;
  return { routes, groups: [...groups.values()], unrouted, edges, values, ops, tableEdges, dlqs, dlqShown, dlqHidden, bw: bwKnown() };
}

// A route's dead-letter topic (or `{schema}`/`{table}` template) from /api/routes; a server from before per-route
// dead-letter topics sends only the global `dead_letter_topic`.
const dlqOf = r => r.dead_letter_topic !== undefined ? r.dead_letter_topic : (plan.dead_letter_topic || null);
// Records acknowledged per second on a dead-letter topic, or on every topic its template rendered.
const dlqTopics = name => Object.keys((rates && rates.topics) || {}).filter(t => globMatch(tplGlob(name), t)).sort();
const dlqRate = name => name.includes('{') ? sumSeries(dlqTopics(name).map(topicRate)) : topicRate(name);
// A dead-letter topic's records acknowledged per op since start (a template: every topic it rendered); null without
// per-op data from the server.
const dlqTotals = name => !(rates && rates.topic_totals) ? null : name.includes('{') ? addSlots(dlqTopics(name).map(topicTotals)) || NO_OPS : topicTotals(name);
// Rows of records acknowledged per op and second, summed over topics, aligned on their newest sample.
const sumRows = list => {
  const n = Math.max(0, ...list.map(a => a.length));
  return Array.from({ length: n }, (_, i) => list.reduce((acc, a) => { const r = a[i - (n - a.length)]; return r ? acc.map((v, j) => v + (r[j] || 0)) : acc; }, NO_OPS));
};
// The dead-letter topic route x uses for its topic `name`: a template is filled from a table the route writes there.
function dlqFor(x, name) {
  const t = x.dlq.includes('{') && x.tables.find(t => t.topic === name);
  return t ? fillTopic(x.dlq, t.name) : x.dlq;
}

// Table, topic and "+N more" cards: name and count on one line, the step line below, the op-share strip down the left edge.
const tableNode = t => `<div data-node="${esc(t.id)}" data-table="${esc(t.name)}" class="${STRIP_CARD} cursor-pointer rounded-sm border ${tableSel === t.name ? 'border-blue' : 'border-ink-600'} bg-ink-900 px-2 py-1 transition-opacity hover:border-blue" style="${STRIP_PAD}" title="${esc(t.name)} · events per second, last 15 s · click for details">${opStrip(t.id)}<div class="flex items-center gap-2"><span class="truncate text-ink-100">${esc(t.name)}</span><span data-v="${esc(t.id)}:rate" class="ml-auto tabular-nums text-ink-300"></span></div>${nodeLine(t.id)}</div>`;
const moreNode = (id, open, n, what) => `<button type="button" data-node="${esc(id)}" data-open="${esc(open)}" class="${id === 'pm:dlq' ? '' : STRIP_CARD + ' '}block w-full rounded-sm border border-dashed border-ink-600 px-2 py-1 text-left text-ink-300 transition-opacity hover:border-blue hover:text-ink-100"${id === 'pm:dlq' ? '' : ` style="${STRIP_PAD}"`} title="${what === 'topics' ? 'Records acknowledged, all of them together' : 'Events, all of them together'}${id.startsWith('tm:') ? ', per second' : ''}">${id === 'pm:dlq' ? '' : opStrip(id)}<span class="flex items-center gap-2">+${n} more ${what}${id.startsWith('tm:') ? `<span data-v="${esc(id)}:rate" class="ml-auto tabular-nums"></span>` : ''}</span>${nodeLine(id)}</button>`;
const lessNode = open => `<button type="button" data-open="${esc(open)}" class="px-2 text-left text-[10px] uppercase tracking-wider text-ink-500 hover:text-ink-100">show fewer</button>`;
const colHead = (title, sub) => `<div class="mb-2 text-[10px] font-medium uppercase tracking-widest text-ink-300">${title}${sub ? ` <span class="normal-case tracking-normal text-ink-500">${sub}</span>` : ''}</div>`;
// Whether a `where` reads row data (`before.`, `after.`, `row.`), the only part that can fail and dead-letter a row;
// quoted text is skipped. A `where` on `op`, `schema` and `table` alone never fails.
const readsRow = w => w != null && /(^|[^\w.])(before|after|row)\./.test(w.replace(/'(?:[^']|'')*'/g, "''"));
// Whether a route can send rows to the dead-letter topic: a `where` on row data, `unchanged = "dead_letter"`, an
// outbox (rows without an aggregate id or payload), `hash` (a value it cannot hash) or `rename` without `include`
// (a name that collides).
const canDeadLetter = r => readsRow(r.where) || r.unchanged === 'dead_letter' || !!r.outbox || !!(r.hash && r.hash.length)
  || !!(r.rename && Object.keys(r.rename).length && !r.include);

// A route card is five aligned rows, SELECT / WHERE / FROM / INTO / PARTITIONED-BY: keyword in a muted column, value
// next to it. An unset clause shows its effective default, dimmed. Each row is one line; the full text is its tooltip.
const SQL_KW = 'text-[10px] leading-4 text-ink-500';
const SQL_VAL = 'flex min-w-0 items-baseline gap-1.5 leading-4';
const sqlTag = (text, title) => `<span class="shrink-0 rounded-sm bg-ink-700 px-1 text-[10px] text-ink-300"${title ? ` title="${esc(title)}"` : ''}>${esc(text)}</span>`;
const sqlText = (text, dim, title) => `<span class="truncate ${dim ? 'text-ink-500' : 'text-ink-100'}" title="${esc(title ?? text)}">${esc(text)}</span>`;
const FROM_TABLES = 3;

// SELECT: what the record value holds. `*` is the envelope; `include` lists columns, `exclude` is `* EXCEPT (...)`,
// then mask/hash/rename as functions and aliases; row format, deletes, tombstones and unchanged are trailing tags.
function selectClause(r) {
  if (r.outbox) return sqlText(r.outbox.payload, false, `outbox payload column ${r.outbox.payload}; key and topic come from the outbox columns`) + sqlTag('outbox');
  const mask = new Set(r.mask || []), hash = new Set(r.hash || []), ren = r.rename || {};
  const col = c => (mask.has(c) ? `mask(${c})` : hash.has(c) ? `hash(${c})` : c) + (ren[c] ? ` AS ${ren[c]}` : '');
  let items;
  if (r.include) items = r.include.map(col);
  else {
    items = [r.exclude ? `* EXCEPT (${r.exclude.join(', ')})` : '*'];
    for (const c of new Set([...mask, ...hash, ...Object.keys(ren)])) items.push(col(c));
  }
  const plain = !r.include && !r.exclude && items.length === 1;
  const text = (r.format === 'row' ? 'ROW ' : '') + items.join(', ');
  const tags = [];
  if (r.format === 'row') tags.push(sqlTag('deletes ' + (r.deletes || 'tombstone'), 'format = "row": what a delete becomes'));
  if (r.tombstones) tags.push(sqlTag('tombstones', 'a null-value record after every delete'));
  if (r.unchanged) tags.push(sqlTag('unchanged ' + r.unchanged, 'rows without their unchanged TOAST columns go to the dead-letter topic'));
  return sqlText(text, plain && r.format !== 'row', plain && r.format !== 'row' ? '* : the change event envelope (before, after, source, op)' : text) + tags.join('');
}

// FROM: the match glob, then the tables it claimed, ` / `-joined, the first few and "+N".
function fromClause(x) {
  const r = x.r, names = x.tables.map(t => t.name).sort();
  if (isOtherwiseRoute(r)) {
    const from = x.from.map(y => y.r.route);
    return `<span class="truncate text-ink-100" title="${esc('Only the rows these routes\' where rejected: ' + from.join(', '))}">otherwise of ${esc(from.join(' / '))}</span>`;
  }
  const glob = r.match == null ? '* (unmatched)' : r.match;
  const listed = names.length === 1 && names[0] === r.match ? [] : names;
  const more = listed.length - FROM_TABLES;
  const title = glob + (names.length ? ': ' + names.join(' / ') : ': no table seen yet');
  // The glob keeps its width; the table list gives way first.
  return `<span class="max-w-full shrink-0 truncate ${r.match == null ? 'text-ink-500' : 'text-ink-100'}" title="${esc(title)}">${esc(glob)}</span>`
    + (listed.length ? `<span class="truncate text-ink-300" title="${esc(title)}">${esc(listed.slice(0, FROM_TABLES).join(' / '))}</span>` : '')
    + (more > 0 ? `<span class="shrink-0 text-ink-500" title="${esc(title)}">+${more}</span>` : '');
}

// INTO: the topic template and, for content routing, ELSE its fallback. The dead-letter topic is in the route's panel.
function intoClause(r) {
  const vals = r.topic_values;
  return sqlText(r.topic, isDefaultRoute(r), isDefaultRoute(r) ? 'sink.topic: ' + r.topic : r.topic)
    + (vals ? sqlTag(vals.length + (vals.length === 1 ? ' value' : ' values'), 'topic_values: ' + vals.join(', ')) : '')
    + (r.topic_fallback ? `<span class="shrink-0 text-[10px] text-ink-500">ELSE</span>${sqlText(r.topic_fallback, false, 'topic_fallback: ' + r.topic_fallback)}` : '');
}

// PARTITIONED-BY: the route's `key`, the outbox aggregate id, else `partition_by` (dimmed: the default).
function partitionClause(r) {
  if (r.key) return sqlText(`key (${r.key.join(', ')})`, false);
  if (r.outbox) return sqlText(r.outbox.aggregate_id, false, `outbox aggregate_id column ${r.outbox.aggregate_id}: the record key and partition`);
  return plan.partition_by === 'table'
    ? sqlText('table', true, 'partition_by = "table": one partition per table')
    : sqlText('primary key', true, 'partition_by = "key": the record key, the table and its primary key');
}

const sqlRow = (kw, val) => `<div class="${SQL_KW}">${kw}</div><div class="${SQL_VAL}">${val}</div>`;

const CARD_METRIC_HELP = {
  processed: 'Every event the route took (its match fits the table)',
  passed: 'Processed minus filtered: events whose where held (or that it could not evaluate, which also count as errors)',
  fallback: 'Sent to topic_fallback (content routing could not use the row value)',
  errors: 'Sent to the dead-letter topic',
};
// More than COLLAPSE_MIN rules: the header offers to collapse every rule card to its name, counts and line. The
// choice is kept per browser.
const COLLAPSE_MIN = 4;
let rulesCollapsed = false;
try { rulesCollapsed = localStorage.getItem('aqueduct_rules_collapsed') === '1'; } catch (_) { /* storage blocked */ }
// Configured rules only: the default route (no match) is not one.
const ruleCount = () => plan ? plan.routes.filter(r => !isDefaultRoute(r)).length : 0;
const rulesFolded = () => rulesCollapsed && ruleCount() >= COLLAPSE_MIN;
$('routecollapse').addEventListener('click', () => {
  rulesCollapsed = !rulesCollapsed;
  try { localStorage.setItem('aqueduct_rules_collapsed', rulesCollapsed ? '1' : '0'); } catch (_) { /* storage blocked */ }
  renderGraph();
});

function routeNode(x, last) {
  const r = x.r, sel = graphSel === x.i, folded = rulesFolded();
  const unseen = x.c.unseen_fields || [];
  return `<div data-node="${x.id}" data-route="${x.i}" class="${STRIP_CARD} cursor-pointer rounded-sm border ${sel ? 'border-blue' : 'border-ink-600'} bg-ink-900 p-2.5 transition-opacity hover:border-blue" style="${STRIP_PAD}">${opStrip(x.id)}`
    // Step and name on top.
    + `<div class="mb-1.5 flex min-w-0 items-center gap-2 border-b border-ink-700 pb-1.5"><span class="flex h-4 min-w-4 items-center justify-center rounded-full bg-ink-700 px-1 text-[10px] text-ink-100" title="${isDefaultRoute(r) ? 'Takes every table no route above matched' : isOtherwiseRoute(r) ? 'Has no match: takes only the rows an earlier route passes on through otherwise' : 'Checked in this order; the first route whose match fits a table owns all its events'}">${isDefaultRoute(r) ? 'else' : x.i + 1}</span>`
    + `<span class="truncate font-sans text-[12px] font-medium text-ink-100" title="${esc(r.route)}">${esc(r.route)}</span></div>`
    + (folded ? '' : `<div class="grid grid-cols-[auto_minmax(0,1fr)] items-baseline gap-x-2">`
      + sqlRow('SELECT', selectClause(r))
      + sqlRow('WHERE', r.where != null ? sqlText(r.where, false) : sqlText('true', true, 'no where: every event passes'))
      + sqlRow('FROM', fromClause(x))
      + sqlRow('INTO', intoClause(r))
      + sqlRow('PARTITIONED-BY', partitionClause(r))
      + (r.otherwise != null ? sqlRow('OTHERWISE', sqlText(r.otherwise, false, 'Rows its where rejects (false or unknown) go on to ' + r.otherwise + '; a type error still dead-letters')) : '')
      + `</div>`)
    // Its rates; the counts are in its panel. Folded, the header's rule already divides them from the name.
    + `<div class="${folded ? '' : 'mt-1.5 border-t border-ink-700 pt-1.5 '}flex flex-wrap gap-x-3 gap-y-0.5 text-[10px]">`
    + `<span title="Matched / processed over the last 5 min: the share of the events it took whose where held"><span class="text-ink-500">match rate</span> <span data-v="${x.id}:matchrate" class="tabular-nums text-ink-100"></span></span>`
    + `<span title="Matched events per second, last 15 s: what it funnels on to its topics (fallback and dead letter included)"><span class="text-ink-500">funneled</span> <span data-v="${x.id}:funnel" class="tabular-nums text-ink-100"></span></span></div>`
    + `<div class="mt-1" title="Events processed per second, last 5 min; click the route for matched, fallback and errors too">${sparkSvg(x.id, 'block w-full', 'height:30px')}</div>`
    + (unseen.length ? `<div class="mt-2 rounded-sm border border-amber/40 bg-amber/10 px-1.5 py-0.5 text-[10px] text-amber" title="Fields this route's where reads that no row has had yet: check the spelling">⚠ never seen: ${esc(unseen.join(', '))}</div>` : '')
    + `</div>`;
}

function topicNodeHtml(id, name, opts) {
  const sel = topicSel === name;
  const tone = (sel ? 'border-blue ' : opts.dlq ? 'border-red/40 ' : 'border-ink-600 ') + (opts.dlq ? 'bg-red/10' : 'bg-ink-900');
  const n = `<span data-v="${esc(id)}:rate" class="ml-auto tabular-nums ${opts.dlq ? 'text-red' : 'text-ink-300'}"></span>`;
  const title = opts.dlq ? `${name} · dead-letter topic${name.includes('{') ? ' template' : ''} · rows its routes could not evaluate, per second · click for details` : `${name} · records acknowledged per second, last 15 s · click for details`;
  return `<div data-node="${esc(id)}" data-topic="${esc(name)}" class="${opts.dlq ? '' : STRIP_CARD + ' '}cursor-pointer rounded-sm border ${tone} px-2 py-1 transition-opacity hover:border-blue"${opts.dlq ? '' : ` style="${STRIP_PAD}"`} title="${esc(title)}">${opts.dlq ? '' : opStrip(id)}<div class="flex items-center gap-2"><span class="truncate ${opts.dlq ? 'text-red' : 'text-ink-100'}">${esc(name)}</span>${n}</div>${nodeLine(id)}</div>`;
}

function graphHtml(m) {
  const tables = m.routes.filter(x => x.tables.length).map(x => `<div class="space-y-1">${x.shown.map(tableNode).join('')}${x.hidden.length ? moreNode(x.moreId, x.id, x.hidden.length, 'tables') : graphOpen.has(x.id) ? lessNode(x.id) : ''}</div>`).join('')
    + (m.unrouted.length ? `<div class="space-y-1"><div class="text-[10px] uppercase tracking-wider text-red">no route · events fail</div>${m.unrouted.map(tableNode).join('')}</div>` : '');
  // The default route (sink.topic) is left out while nothing has reached it.
  const routes = m.routes.filter(x => !isDefaultRoute(x.r) || x.tables.length || (x.c.matched || 0) > 0).map((x, i, all) => routeNode(x, i === all.length - 1)).join('');
  const topics = m.groups.map(g => `<div class="space-y-1">`
    + g.shown.map(t => topicNodeHtml('p:' + t, t, {})).join('')
    + (g.hidden.length ? moreNode(g.moreId, 'g:' + g.tpl, g.hidden.length, 'topics') : graphOpen.has('g:' + g.tpl) ? lessNode('g:' + g.tpl) : '')
    + `</div>`).join('')
    + (m.dlqs.size ? `<div class="mt-2 space-y-1"><div class="text-[10px] uppercase tracking-wider text-red">dead letter</div>${m.dlqShown.map(d => topicNodeHtml(d.id, d.name, { dlq: true })).join('')}`
      + (m.dlqHidden.length ? moreNode('pm:dlq', 'g:dlq', m.dlqHidden.length, 'dead-letter topics') : graphOpen.has('g:dlq') ? lessNode('g:dlq') : '') + `</div>` : '');
  return `<div class="flex flex-col">${colHead('Tables')}<div class="flex flex-1 flex-col justify-center gap-3">${tables || '<div class="text-ink-500">no events yet</div>'}</div></div>`
    + `<div class="flex flex-col">${colHead('Routes', 'first match wins')}<div class="flex flex-1 flex-col justify-center gap-3">${routes}</div></div>`
    + `<div class="flex flex-col">${colHead('Topics')}<div class="flex flex-1 flex-col justify-center gap-3">${topics}</div></div>`;
}

// What hovering a card lights up. A table: its edge to its route, the route, the route's edge to its topic, the topic.
// Any other card: its own edges and the cards at their other end.
function graphPath(m, node) {
  const on = new Set([node]);
  const ids = node.startsWith('t:') && m.tableEdges[node.slice(2)];
  if (ids) {
    for (const id of ids) { on.add(id); for (const n of id.split('>')) on.add(n); }
    return on;
  }
  for (const e of m.edges) if (e.from === node || e.to === node) { on.add(e.id); on.add(e.from); on.add(e.to); }
  return on.size > 1 ? on : null;
}

// The edge layers are redrawn every second; their elements are kept by key and only their attributes change, so the
// flowing dots keep moving and a hovered card's labels do not blink.
const SVG_NS = 'http://www.w3.org/2000/svg';
const setAttrs = (el, attrs) => { for (const [k, v] of Object.entries(attrs)) if (el.getAttribute(k) !== String(v)) el.setAttribute(k, v); };
// A keyed child list of `parent`: get(key, tag) finds or makes the child, done() drops the ones not asked for.
function keyed(parent) {
  const old = new Map([...parent.children].map(c => [c.dataset.k, c])), used = new Set();
  return {
    get(key, tag) {
      used.add(key);
      let el = old.get(key);
      if (!el) { el = document.createElementNS(SVG_NS, tag); el.dataset.k = key; parent.appendChild(el); }
      return el;
    },
    done() { for (const [k, el] of old) if (!used.has(k)) el.remove(); },
  };
}
// Lines first, the flowing dots over them.
function edgeLayers(svg) {
  if (svg.children.length !== 2) svg.innerHTML = '<g></g><g></g>';
  return [svg.children[0], svg.children[1]];
}

function drawEdges() {
  const m = graphModel, g = $('graph'), svg = $('graphedges');
  if (!m) { svg.innerHTML = $('graphlabels').innerHTML = ''; return; }
  const [lineLayer, flowLayer] = edgeLayers(svg);
  const lines = keyed(lineLayer), flows = keyed(flowLayer), texts = keyed($('graphlabels'));
  const box = g.getBoundingClientRect(), els = {};
  for (const el of g.querySelectorAll('[data-node]')) els[el.dataset.node] = el;
  const at = (id, right) => {
    const el = els[id];
    if (!el) return null;
    const r = el.getBoundingClientRect();
    return [(right ? r.right : r.left) - box.left, r.top + r.height / 2 - box.top];
  };
  const hl = graphHover && graphPath(m, graphHover);
  for (const [id, el] of Object.entries(els)) el.classList.toggle('opacity-40', !!hl && !hl.has(id));
  // Widths follow bandwidth (bytes per second now), or, from a server that counts no bytes, records since start.
  const size = e => m.bw ? e.bw : e.w;
  const max = Math.max(...m.edges.filter(e => !e.chain).map(size), 0) || 1;
  const now = Date.now();
  for (const e of m.edges) {
    if (e.chain) { drawChain(e, at, lines, flows, texts, !hl || hl.has(e.id), now); continue; }
    const a = at(e.from, true), b = at(e.to, false);
    if (!a || !b) continue;
    const mx = ((a[0] + b[0]) / 2).toFixed(1);
    const d = `M${a[0].toFixed(1)},${a[1].toFixed(1)} C${mx},${a[1].toFixed(1)} ${mx},${b[1].toFixed(1)} ${b[0].toFixed(1)},${b[1].toFixed(1)}`;
    const on = !hl || hl.has(e.id);
    const width = size(e) > 0 ? 1.5 + 4.5 * Math.sqrt(size(e) / max) : 1;
    const color = e.dlq ? EDGE_RED : hl && on ? EDGE_HL : EDGE_BASE;
    setAttrs(lines.get(e.id, 'path'), { d, fill: 'none', stroke: color, 'stroke-width': width.toFixed(1), 'stroke-opacity': on ? (e.dlq ? 0.7 : 1) : 0.15 });
    // Recent traffic: a dotted line that flows (static when the viewer prefers reduced motion).
    if ((flowUntil[e.id] || 0) > now) {
      setAttrs(flows.get(e.id, 'path'), { d, fill: 'none', stroke: e.dlq ? EDGE_RED : EDGE_FLOW, 'stroke-width': 1.5, 'stroke-linecap': 'round',
        'stroke-dasharray': '2 10', 'stroke-opacity': on ? 0.9 : 0.15, class: 'motion-safe:animate-flow' });
    }
    // A hovered card's pathways carry their bandwidth at the curve's midpoint, in the gap between the columns, on a
    // layer above the cards. A server that counts no bytes gets no labels.
    if (hl && on && m.bw) {
      const t = texts.get(e.id, 'text'), s = bwText(e.bw);
      setAttrs(t, { x: mx, y: ((a[1] + b[1]) / 2).toFixed(1), dy: '0.35em', 'text-anchor': 'middle', 'font-family': 'var(--font-mono)', 'font-size': 10,
        fill: e.dlq ? EDGE_RED : TABLE_LINE, stroke: 'var(--color-ink-900)', 'stroke-width': 4, 'stroke-linejoin': 'round', 'paint-order': 'stroke' });
      if (t.textContent !== s) t.textContent = s;
    }
  }
  lines.done(); flows.done(); texts.done();
}

// An otherwise edge: from the left of one route card down (or up) to the left of the later one it passes rows on to,
// bulging out into the gap by CHAIN_BULGE, thin and dashed, its passed-on count written up its side.
const CHAIN_BULGE = 22;
function drawChain(e, at, lines, flows, texts, on, now) {
  const a = at(e.from, false), b = at(e.to, false);
  if (!a || !b) return;
  const x = Math.min(a[0], b[0]) - CHAIN_BULGE;
  const d = `M${a[0].toFixed(1)},${a[1].toFixed(1)} C${x.toFixed(1)},${a[1].toFixed(1)} ${x.toFixed(1)},${b[1].toFixed(1)} ${b[0].toFixed(1)},${b[1].toFixed(1)}`;
  setAttrs(lines.get(e.id, 'path'), { d, fill: 'none', stroke: on && graphHover ? EDGE_HL : EDGE_FLOW, 'stroke-width': 1, 'stroke-dasharray': '4 3', 'stroke-opacity': on ? 0.8 : 0.15 });
  if ((flowUntil[e.id] || 0) > now) {
    setAttrs(flows.get(e.id, 'path'), { d, fill: 'none', stroke: EDGE_FLOW, 'stroke-width': 1.5, 'stroke-linecap': 'round',
      'stroke-dasharray': '2 10', 'stroke-opacity': on ? 0.9 : 0.15, class: 'motion-safe:animate-flow' });
  }
  // The curve's leftmost point is 3/4 of the way out; the label sits just outside it, reading upwards.
  const lx = (a[0] + 3 * (x - a[0]) / 4 - 4).toFixed(1), ly = ((a[1] + b[1]) / 2).toFixed(1);
  const t = texts.get(e.id, 'text'), s = 'passed on ' + fmt(e.w);
  setAttrs(t, { x: lx, y: ly, 'text-anchor': 'middle', transform: `rotate(-90 ${lx} ${ly})`, 'font-family': 'var(--font-mono)', 'font-size': 9,
    fill: EDGE_FLOW, 'fill-opacity': on ? 1 : 0.3, stroke: 'var(--color-ink-900)', 'stroke-width': 3, 'stroke-linejoin': 'round', 'paint-order': 'stroke' });
  if (t.textContent !== s) t.textContent = s;
}

// Marks the edges whose counts moved since the last frame as having recent traffic.
function trackFlow(m) {
  const now = Date.now(), cur = { t: {}, r: [] };
  for (const x of m.routes) {
    cur.r[x.i] = { produced: x.c.produced || 0, errors: x.c.errors || 0, passed: x.c.passed_on || 0 };
    const was = flowPrev && flowPrev.r[x.i];
    let moved = false;
    for (const t of x.tables) {
      cur.t[t.name] = t.events;
      if (!flowPrev || !(t.events > (flowPrev.t[t.name] ?? t.events))) continue;
      const [into, out] = m.tableEdges[t.name];
      flowUntil[into] = now + FLOW_MS;
      // A route that picks its topic per row has no single topic edge per table: its route → topic edges flow below.
      if (out && was && cur.r[x.i].produced > was.produced) { flowUntil[out] = now + FLOW_MS; moved = true; }
    }
    if (was && cur.r[x.i].produced > was.produced && !moved) for (const o of x.out) flowUntil[o.id] = now + FLOW_MS;
    if (was && x.dlqEdge && cur.r[x.i].errors > was.errors) flowUntil[x.dlqEdge] = now + FLOW_MS;
    if (was && x.chainEdge && cur.r[x.i].passed > was.passed) flowUntil[x.chainEdge] = now + FLOW_MS;
  }
  flowPrev = cur;
}

// The routes that write to a topic: by its template, as their fallback, or (dead-letter topic) on errors.
const topicRoutes = (m, name) => m.routes.filter(x => x.r.topic === name || x.topics.has(name) || x.r.topic_fallback === name);
const closeBtn = '<button type="button" id="routeclose" class="ml-auto rounded-sm border border-ink-600 bg-ink-700 px-2 py-0.5 uppercase tracking-wider text-ink-100 hover:border-blue">Close</button>';
// The larger step-line chart in the detail panel; drawn by drawDetail on every frame.
// The detail chart under its controls, with its unit (`yLabel`) written up the y axis.
const detailChart = (legend, yLabel) => `<div class="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-ink-300">${legend}<span id="detailstats" class="ml-auto tabular-nums text-ink-200"></span></div>`
  + `<div class="mt-1 flex"><span class="shrink-0 text-center text-ink-500" style="writing-mode:vertical-rl;transform:rotate(180deg);font-size:10px">${esc(yLabel)}</span>`
  + `<svg id="detailchart" viewBox="0 0 800 160" preserveAspectRatio="none" class="block h-40 min-w-0 flex-1"></svg></div>`;
const legendItem = (color, text) => `<span class="flex items-center gap-1.5"><i class="inline-block h-0.5 w-3" style="background:${color}"></i>${esc(text)}</span>`;

const routeLink = x => `<button type="button" data-route="${x.i}" class="mr-3 text-blue hover:underline">${esc(x.r.route)}</button>`;
const inspectBtn = name => `<button type="button" data-inspect="${esc(name)}" class="rounded-sm border border-ink-600 bg-ink-700 px-2 py-0.5 uppercase tracking-wider text-ink-100 hover:border-blue">Open in inspector</button>`;
function topicDetail(m, name) {
  const row = (k, v) => `<div class="flex gap-3"><span class="w-24 shrink-0 text-ink-500">${k}</span><span class="min-w-0 break-words text-ink-100">${v}</span></div>`;
  const can = !!(lastData && lastData.inspector);
  const dlq = m.dlqs.get(name), tpl = name.includes('{');
  const head = `<div class="mb-2 flex flex-wrap items-center gap-3"><span class="break-all font-sans text-[12px] font-medium text-ink-100">${esc(name)}</span><span class="text-ink-500">${dlq ? 'dead-letter topic' + (tpl ? ' template' : '') : 'topic'}</span>`
    + (tpl ? '' : can ? inspectBtn(name) : '<span class="text-ink-500">inspector off</span>') + closeBtn + `</div><div class="space-y-1">`;
  if (dlq) {
    // What it serves: each route, and the topics it writes (a template's own topics are the rendered ones).
    const served = dlq.routes.map(x => routeLink(x) + `<span class="mr-3 text-ink-500">${esc(x.topics.size ? [...x.topics.keys()].sort().join(', ') : x.r.topic)}</span>`).join('');
    const rendered = tpl ? dlqTopics(name) : [];
    // A wider label column, so "dead letter for" stays on one line.
    const wide = (k, v) => `<div class="flex gap-3"><span class="w-28 shrink-0 text-ink-500">${k}</span><span class="min-w-0 break-words text-ink-100">${v}</span></div>`;
    $('routedetail').innerHTML = head
      + wide('dead letter for', served)
      + (tpl ? wide('topics', rendered.length ? rendered.map(t => can ? `<button type="button" data-inspect="${esc(t)}" class="mr-3 text-blue hover:underline" title="Open in inspector">${esc(t)}</button>` : `<span class="mr-3">${esc(t)}</span>`).join('') : '<span class="text-ink-500">none created yet: one per table, on its first dead-lettered row</span>') : '')
      + wide('acknowledged', '<span id="detailops">–</span>')
      + `</div>` + detailChart(opButtons(dlqTotals(name)), 'records acknowledged / s');
    return;
  }
  const writers = topicRoutes(m, name);
  // The dead-letter topic of each route writing here that can dead-letter a row: click to select it in the graph.
  const dead = [...new Map(writers.filter(x => x.dlq).map(x => [dlqFor(x, name), x.dlq])).entries()];
  $('routedetail').innerHTML = head
    + row('written by', writers.length ? writers.map(routeLink).join('') : '<span class="text-ink-500">no route seen writing to it</span>')
    + row('dead letter', dead.length ? dead.map(([shown, node]) => `<button type="button" data-dlq="${esc(node)}" class="mr-3 text-red hover:underline" title="${esc(shown === node ? 'Select the dead-letter topic' : 'Select ' + node + ', the template it is rendered from')}">${esc(shown)}</button>`).join('')
      : `<span class="text-ink-500">${writers.length ? 'none: no route writing here can dead-letter a row' : '–'}</span>`)
    + row('acknowledged', '<span id="detailops">–</span>')
    + `</div>` + detailChart(opButtons(topicTotals(name)), 'records acknowledged / s');
}

// A table's panel (source side, no Kafka details): the route that claims it, its events, how far it has got, and its
// rate per operation. The block below the header is refreshed by every stream frame.
function fillTableMarks() {
  const el = document.getElementById('tablehwm');
  const d = lastData;
  if (!el || !d || tableSel == null) return;
  const name = tableSel, m = (d.table_marks || {})[name] || {};
  const row = (label, v, title) => `<div class="flex gap-3"${title ? ` title="${esc(title)}"` : ''}><span class="w-24 shrink-0 text-ink-500">${label}</span><span class="min-w-0 break-words text-ink-100">${v}</span></div>`;
  const ops = (d.table_ops || {})[name] || [0, 0, 0, 0, 0], total = d.events_total || 1, events = (d.tables || {})[name] || 0;
  const fresh = Math.max(0, ...Object.values(d.table_marks || {}).map(x => x.last_commit_ms || 0));
  const behind = m.last_commit_ms ? fresh - m.last_commit_ms : 0;
  const tone = d.state !== 'streaming' ? '' : behind > BEHIND_RED_MS ? 'text-red' : behind > BEHIND_AMBER_MS ? 'text-amber' : '';
  el.innerHTML = row('ops', ['insert', 'update', 'delete', 'truncate', 'read'].map((o, i) => `${o} ${fmt(ops[i])}`).join(' · ') + ` <span class="text-ink-500">· ${(100 * events / total).toFixed(1)}% of all events</span>`)
    + row('last LSN', esc(m.last_lsn || '–'))
    + row('last commit', m.last_commit_ms ? `<span class="${tone}">${esc(ago(m.last_commit_ms))}</span> <span class="text-ink-500">${esc(isoMs(m.last_commit_ms))}</span>` : '–',
      'Age of the newest delivered change\'s commit. Amber: more than 1 min behind the freshest table, red: more than 10 min (only while streaming).')
    + row('delivered', m.last_delivered_ms ? `${esc(ago(m.last_delivered_ms))} <span class="text-ink-500">${esc(isoMs(m.last_delivered_ms))}</span>` : '–');
}

function tableDetail(m, name) {
  const row = (k, v) => `<div class="flex gap-3"><span class="w-24 shrink-0 text-ink-500">${k}</span><span class="min-w-0 break-words text-ink-100">${v}</span></div>`;
  const t = m.routes.flatMap(x => x.tables).concat(m.unrouted).find(t => t.name === name);
  const x = t && t.route != null ? m.routes[t.route] : null;
  $('routedetail').innerHTML = `<div class="mb-2 flex flex-wrap items-center gap-3"><span class="break-all font-sans text-[12px] font-medium text-ink-100">${esc(name)}</span><span class="text-ink-500">table</span>${closeBtn}</div><div class="space-y-1">`
    + row('claimed by', x ? `<button type="button" data-route="${x.i}" class="text-blue hover:underline">${esc(x.r.route)}</button>` : '<span class="text-red">no route · its events fail</span>')
    + row('events', `<span data-v="${esc('t:' + name)}" class="tabular-nums">${fmt(t ? t.events : 0)}</span>`)
    + `</div><div id="tablehwm" class="mt-1 space-y-1"></div>` + detailChart(opButtons(((lastData && lastData.table_ops) || {})[name] || null), 'events / s');
}

function routeDetail(m) {
  const x = graphSel == null ? null : m.routes[graphSel];
  $('routedetail').hidden = !x && topicSel == null && tableSel == null;
  if (!x) { if (topicSel != null) topicDetail(m, topicSel); else if (tableSel != null) { tableDetail(m, tableSel); fillTableMarks(); } return; }
  // Only what the card does not already show: the card has SELECT/WHERE/FROM/INTO/PARTITIONED-BY, the outcome
  // counts and the op bar.
  const r = x.r, row = (k, v) => `<div class="flex gap-3"><span class="w-24 shrink-0 text-ink-500">${k}</span><span class="min-w-0 break-words text-ink-100">${v}</span></div>`;
  const ON_CARD = ['route', 'match', 'where', 'topic', 'counts', 'unseen_fields', 'include', 'exclude', 'mask', 'hash', 'rename',
    'format', 'deletes', 'tombstones', 'unchanged', 'key', 'outbox', 'topic_fallback', 'topic_values', 'dead_letter_topic', 'otherwise'];
  const extra = Object.keys(r).filter(k => !ON_CARD.includes(k) && r[k] != null);
  const rendered = isTemplate(r.topic) || (r.topic_fallback != null) ? [...x.topics.keys()].filter(t => t !== r.topic_fallback).sort() : [];
  $('routedetail').innerHTML = `<div class="mb-2 flex items-center gap-3"><span class="font-sans text-[12px] font-medium text-ink-100">${esc(r.route)}</span><span class="text-ink-500">${isDefaultRoute(r) ? 'default · takes what no route matched' : 'step ' + (x.i + 1) + ' of ' + m.routes.length + (isOtherwiseRoute(r) ? ' · reached only through otherwise' : '')}</span>${closeBtn}</div><div class="space-y-1">`
    + row('counts', CARD_METRICS.map(([k, , label]) => `<span class="mr-3" title="${esc(CARD_METRIC_HELP[k])}"><span class="text-ink-500">${label || k}</span> <span data-v="${x.id}:${k}" class="tabular-nums"></span></span>`).join(''))
    + row('produced', `<span data-v="${x.id}:produced" class="tabular-nums">${fmt(x.c.produced || 0)}</span> <span class="text-ink-500">records written to its topics</span>`)
    + row('produced by op', '<span id="detailops">–</span>')
    + (x.next ? row('passed on', `<span data-v="${x.chainEdge}" class="tabular-nums"></span> <span class="text-ink-500">of the filtered, to</span> ${routeLink(x.next)}`) : '')
    + (x.from.length ? row('passed on from', x.from.map(routeLink).join('')) : '')
    + (r.topic_fallback != null ? row('topic fallback', esc(r.topic_fallback)) : '')
    + (r.topic_values && r.topic_values.length ? row('topic values', esc(r.topic_values.join(', '))) : '')
    + (rendered.length ? row('topics written', esc(rendered.join(', '))) : '')
    + (x.dlq ? row('dead letter', x.dlqNames.length ? x.dlqNames.map(n => `<button type="button" data-dlq="${esc(n)}" class="mr-3 text-red hover:underline">${esc(n)}</button>`).join('') : esc(x.dlq)) : '')
    + (x.tables.length > FROM_TABLES ? row('all tables', esc(x.tables.map(t => t.name).sort().join(', '))) : '')
    + extra.map(k => row(esc(k), esc(JSON.stringify(r[k])))).join('')
    + `</div>` + detailChart(lineSelect() + opButtons(routeOpTotals(x, routeRate(x.i))), 'events / s');
}

// The detail panel's chart: one step line, the records per second of the ops toggled on, summed (tables: events,
// routes: produced, topics: acknowledged); and the panel's per-op totals. Without per-op history (an older server)
// a route or topic line counts every op.
// Every card's step line from the current rate window.
function drawCardSparks(m) {
  const lines = {};
  for (const x of m.routes) {
    const rr = routeRate(x.i);
    // Processed (every event it took = produced + filtered + fallback + errors), matched (processed minus filtered),
    // fallback and errors: one line each, in the colours of the card's metrics.
    if (rr) {
      const processed = sumSeries(ROUTE_COUNTS.map(k => rr[k] || [])), passed = sumSeries([rr.produced || [], rr.fallback || [], rr.errors || []]);
      const series = { processed, passed, fallback: rr.fallback || [], errors: rr.errors || [] };
      lines[x.id] = [[series.processed, TABLE_LINE, 1.5]];
    }
    for (const t of x.shown) lines[t.id] = [[tableEventsRate(t.name), TABLE_LINE, 1.5]];
    if (x.hidden.length) lines[x.moreId] = [[sumSeries(x.hidden.map(t => tableEventsRate(t.name))), TABLE_LINE, 1.5]];
  }
  for (const g of m.groups) {
    for (const t of g.shown) lines['p:' + t] = [[topicRate(t), TOPIC_LINE, 1.5]];
    if (g.hidden.length) lines[g.moreId] = [[sumSeries(g.hidden.map(topicRate)), TOPIC_LINE, 1.5]];
  }
  for (const t of m.unrouted) lines[t.id] = [[tableEventsRate(t.name), RATE_LINE.errors, 1.5]];
  for (const d of m.dlqShown) lines[d.id] = [[dlqRate(d.name), RATE_LINE.errors, 1.5]];
  if (m.dlqHidden.length) lines['pm:dlq'] = [[sumSeries(m.dlqHidden.map(d => dlqRate(d.name))), RATE_LINE.errors, 1.5]];
  for (const el of document.querySelectorAll('#routing [data-spark]')) { const l = lines[el.dataset.spark]; el.innerHTML = l ? spark(l) : ''; }
}

// An op that happens for the first time gets its button without reopening the panel.
function refreshOpButtons(totals) {
  const btns = document.getElementById('detailopbtns');
  if (btns && btns.dataset.mask !== opMask(totals)) btns.outerHTML = opButtons(totals);
}

function drawDetail(m) {
  const dc = $('detailchart');
  if (!dc) return;
  let rows = null, all = [], color = TOPIC_LINE, totals = null;
  if (topicSel != null) {
    const dead = m.dlqs.has(topicSel);
    rows = rates && rates.topic_ops ? (dead && topicSel.includes('{') ? sumRows(dlqTopics(topicSel).map(t => rates.topic_ops[t] || [])) : rates.topic_ops[topicSel] || []) : null;
    all = dead ? dlqRate(topicSel) : topicRate(topicSel);
    color = dead ? RATE_LINE.errors : TOPIC_LINE;
    totals = dead ? dlqTotals(topicSel) : topicTotals(topicSel);
  } else if (tableSel != null) {
    rows = ((lastData && lastData.history_ops) || {})[tableSel] || [];
    color = TABLE_LINE;
  } else if (graphSel != null) {
    // A route: four lines, processed, matched, fallback and errors, from the route's own counters, each counting only
    // the ops toggled on (without per-op history, every op).
    const x = m.routes[graphSel];
    if (!x) return;
    const rr = routeRate(x.i) || {};
    const tot = routeOpTotals(x, rr), sel = [...detailOps].filter(op => !tot || tot[op] > 0).sort();
    const pick = (k, rows) => tot ? (rr[rows] || []).map(o => sel.reduce((a, op) => a + (o[op] || 0), 0)) : rr[k] || [];
    const [produced, filtered, fallback, errors] = ['ops', ...ROUTE_OUTCOME_OPS].map((rows, j) => pick(ROUTE_COUNTS[j], rows));
    const processed = sumSeries([produced, filtered, fallback, errors]);
    const series = { processed, passed: sumSeries([produced, fallback, errors]), fallback, errors };
    const [k, color, label] = CARD_METRICS[detailLine];
    // Several ops: the outcome stacked by op, in the op colours (the op buttons are its legend); one op, or no per-op
    // history, a line in the outcome's colour.
    if (tot && sel.length > 1) {
      const parts = { processed: ['ops', ...ROUTE_OUTCOME_OPS], passed: ['ops', 'fallback_ops', 'errors_ops'], fallback: ['fallback_ops'], errors: ['errors_ops'] }[k];
      const rows = sumRows(parts.map(p => rr[p] || []));
      chart(sel.map(op => ({ op, h: rows.map(o => o[op] || 0) })), dc, OPLINE, 160, true);
    } else chart([{ op: 0, h: series[k] }], dc, [color], 160);
    $('detailstats').textContent = tot && !sel.length ? 'no operation selected'
      : (tot ? '' : 'all operations (no per-op history from this server) · ') + (label || k) + ' ' + rateStats(series[k]);
    refreshOpButtons(tot);
    const el = document.getElementById('detailops');
    const t = opSlots(x.c.ops);
    if (el) el.textContent = t ? OP_LABEL.map((k, i) => `${k} ${fmt(t[i])}`).join(' · ') : 'no per-operation counts from this server';
    return;
  } else return;
  // One line per selected op, in the op colours (the buttons are the legend); without per-op history, one line.
  // Only ops that have happened get a line (and a button).
  const seen = tableSel != null ? ((lastData && lastData.table_ops) || {})[tableSel] || null : totals;
  const sel = [...detailOps].filter(op => !seen || seen[op] > 0).sort();
  const perOp = rows ? sel.map(op => ({ op, h: rows.map(o => o[op] || 0) })) : null;
  const h = perOp ? sumSeries(perOp.map(s => s.h)) : all;
  // Several ops stack (the top edge is their sum); one op is a plain line.
  if (perOp) chart(perOp, dc, OPLINE, 160, perOp.length > 1); else chart([{ op: 0, h }], dc, [color], 160);
  $('detailstats').textContent = rows && !sel.length ? 'no operation selected'
    : (rows ? '' : 'all operations (no per-op history from this server) · ') + rateStats(h);
  refreshOpButtons(seen);
  const el = document.getElementById('detailops');
  if (el) el.textContent = totals ? `${fmt(totals.reduce((a, n) => a + n, 0))} · ` + OP_LABEL.map((k, i) => `${k} ${fmt(totals[i])}`).join(' · ') : 'no per-operation counts from this server';
}

function renderGraph(frame) {
  if (!plan) {
    $('graphcols').textContent = planError || 'loading route plan…';
    $('graphedges').innerHTML = $('graphlabels').innerHTML = '';
    return;
  }
  const m = graphModelOf(lastData);
  if (frame) trackFlow(m);
  const key = JSON.stringify([m.routes.map(x => [x.shown.map(t => t.name), x.hidden.length, x.c.unseen_fields]),
    m.groups.map(g => [g.shown, g.hidden.length]), m.unrouted.map(t => t.name), m.dlqShown.map(d => [d.name, d.routes.length]), m.dlqHidden.length,
    topicSel && m.dlqs.has(topicSel) && topicSel.includes('{') ? dlqTopics(topicSel) : null, graphSel, topicSel, tableSel, [...graphOpen], !!(lastData && lastData.inspector),
    rulesFolded()]);
  const btn = $('routecollapse');
  btn.hidden = ruleCount() < COLLAPSE_MIN;
  btn.textContent = rulesFolded() ? 'Expand rules' : 'Collapse rules';
  btn.setAttribute('aria-pressed', String(rulesFolded()));
  graphModel = m;
  if (key !== graphKey) {
    graphKey = key;
    $('graphcols').innerHTML = graphHtml(m);
    routeDetail(m);
  }
  for (const el of document.querySelectorAll('#routing [data-v]')) {
    const v = m.values[el.dataset.v];
    if (v != null) el.textContent = v;
    if (el.dataset.v.endsWith(':errors')) el.classList.toggle('text-red', v != null && v !== '0');
  }
  for (const el of document.querySelectorAll('#routing [data-opstrip]')) {
    const id = el.dataset.opstrip;
    fillOpStrip(el, m.ops[id], id.startsWith('r:') ? 'produced' : /^(p:|pm:|dlq:)/.test(id) ? 'records acknowledged' : 'events');
  }
  drawCardSparks(m);
  const warn = m.routes.reduce((a, x) => a + (x.c.unseen_fields || []).length, 0);
  $('routewarn').hidden = !warn;
  $('routewarn').textContent = warn + (warn === 1 ? ' field never seen' : ' fields never seen');
  // Dead-letter topics: the global override, else each route's own (`<topic>.dlq` unless the route sets one).
  const perRoute = plan.routes.some(r => r.dead_letter_topic !== undefined);
  $('routenote').textContent = 'first match wins · partition by ' + plan.partition_by
    + (plan.dead_letter_topic ? ' · dead letter ' + plan.dead_letter_topic : perRoute ? ' · dead letter <topic>.dlq per route' : '');
  drawDetail(m);
  drawEdges();
}

async function loadPlan() {
  try {
    const r = await fetch('/api/routes', authInit());
    if (r.status === 401 || r.status === 403) { planError = r.status === 401 ? 'Unauthorized. Open the page once with #token=...' : 'Routing plan not available'; renderGraph(); return; }
    if (r.status === 404) { planError = 'No route plan yet'; renderGraph(); setTimeout(loadPlan, 5000); return; }
    if (!r.ok) throw new Error('routes ' + r.status);
    plan = await r.json();
    planError = null;
    renderGraph();
    loadRates();
  } catch (_) { setTimeout(loadPlan, 5000); }
}

function inspectTopic(name) {
  const sel = $('evtopic');
  if (![...sel.options].some(o => o.value === name)) sel.add(new Option(name, name));
  sel.value = name;
  $('inspector').hidden = false;
  document.body.classList.add('overflow-hidden');
  $('evsearch').focus();
  sampleEvents();
}

$('graph').addEventListener('click', ev => {
  const open = ev.target.closest('[data-open]');
  if (open) { graphOpen.has(open.dataset.open) ? graphOpen.delete(open.dataset.open) : graphOpen.add(open.dataset.open); renderGraph(); return; }
  const table = ev.target.closest('[data-table]');
  if (table) { tableSel = tableSel === table.dataset.table ? null : table.dataset.table; graphSel = topicSel = null; renderGraph(); return; }
  const topic = ev.target.closest('[data-topic]');
  if (topic) { topicSel = topicSel === topic.dataset.topic ? null : topic.dataset.topic; graphSel = tableSel = null; renderGraph(); return; }
  const route = ev.target.closest('[data-route]');
  if (route) { graphSel = graphSel === +route.dataset.route ? null : +route.dataset.route; topicSel = tableSel = null; renderGraph(); }
});
$('routedetail').addEventListener('change', ev => {
  if (ev.target.id !== 'detailline') return;
  detailLine = +ev.target.value;
  ev.target.outerHTML = lineSelect();
  if (graphModel) drawDetail(graphModel);
});
$('routedetail').addEventListener('click', ev => {
  const opBtn = ev.target.closest('[data-op]');
  if (opBtn) {
    const op = +opBtn.dataset.op, on = !detailOps.has(op);
    on ? detailOps.add(op) : detailOps.delete(op);
    opBtn.setAttribute('aria-pressed', String(on));
    opBtn.outerHTML = `<button type="button" data-op="${op}" aria-pressed="${on}" ${opBtnAttrs(op, on)}>${OP_LABEL[op]}</button>`;
    if (graphModel) drawDetail(graphModel);
    return;
  }
  if (ev.target.closest('#routeclose')) { graphSel = topicSel = tableSel = null; renderGraph(); return; }
  // A dead-letter topic named in a topic's panel: select its node (opening "+N more" if it is behind it).
  const dead = ev.target.closest('[data-dlq]');
  if (dead) {
    const name = dead.dataset.dlq;
    if (graphModel && graphModel.dlqHidden.some(d => d.name === name)) graphOpen.add('g:dlq');
    topicSel = name; graphSel = tableSel = null; renderGraph(); return;
  }
  const insp = ev.target.closest('[data-inspect]');
  if (insp) { inspectTopic(insp.dataset.inspect); return; }
  const route = ev.target.closest('[data-route]');
  if (route) { graphSel = +route.dataset.route; topicSel = tableSel = null; renderGraph(); }
});
// Hover: a card lights up at once; the gap between two cards keeps the last one lit for a moment, so moving from card
// to card does not fade every card in and out.
const HOVER_GRACE_MS = 250;
let hoverTimer = 0;
function setHover(node) {
  clearTimeout(hoverTimer);
  if (node !== graphHover) { graphHover = node; drawEdges(); }
}
$('graph').addEventListener('pointerover', ev => {
  const n = ev.target.closest('[data-node]');
  if (n) setHover(n.dataset.node);
  else if (graphHover) { clearTimeout(hoverTimer); hoverTimer = setTimeout(() => setHover(null), HOVER_GRACE_MS); }
});
$('graph').addEventListener('pointerleave', () => setHover(null));
new ResizeObserver(drawEdges).observe($('graph'));
loadPlan();

// SSE over fetch: EventSource cannot set headers. Each frame is a full snapshot, so a reconnect
// needs no resume position, only a backoff. The server sends a frame every second, so STALL_MS
// without any bytes means the connection is hung: abort it and reconnect.
const STALL_MS = 10000;
async function streamLoop() {
  let delay = 1000;
  for (;;) {
    const ctl = new AbortController();
    let stall = setTimeout(() => ctl.abort(), STALL_MS);
    try {
      const r = await fetch('/api/stream', authInit({ signal: ctl.signal }));
      if (r.status === 401 || r.status === 403) { clearTimeout(stall); setConn(false, r.status === 401 ? 'unauthorized' : 'disabled'); return; }
      if (!r.ok || !r.body) throw new Error('stream ' + r.status);
      setConn(true, 'live');
      delay = 1000;
      const reader = r.body.getReader(), dec = new TextDecoder();
      let buf = '';
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        clearTimeout(stall);
        stall = setTimeout(() => ctl.abort(), STALL_MS);
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf('\n\n')) >= 0) {
          const frame = buf.slice(0, i);
          buf = buf.slice(i + 2);
          const data = frame.split('\n').filter(l => l.startsWith('data:')).map(l => l.slice(5).replace(/^ /, '')).join('\n');
          // A drawing bug in one card must not look like a dropped connection: log it and keep streaming.
          if (data) {
            let snap;
            try { snap = JSON.parse(data); } catch (_) { continue; }
            try { render(snap); } catch (e) { console.error('cockpit render failed', e); }
          }
        }
      }
    } catch (_) { /* fall through to reconnect */ }
    clearTimeout(stall);
    ctl.abort();
    resetSmoothing();
    setConn(false, 'reconnecting');
    await new Promise(res => setTimeout(res, delay));
    delay = Math.min(delay * 2, 15000);
  }
}
streamLoop();
