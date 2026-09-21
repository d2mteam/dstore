// Trang của một node. Tình trạng và cấu hình đọc từ API riêng của UI
// (api/...); danh sách shard đọc thẳng từ API của node (../shards).
import { $, api, get, html, render, bytes, ago, when, meter, toast, busy, poll } from './common.js';

const MiB = 1024 * 1024;
const MAX_SHARDS = 500; // node có thể giữ hàng trăm nghìn shard; chỉ vẽ chừng này dòng

let applied = null;   // cấu hình đang có hiệu lực trên node
let dirty = false;    // người dùng đang sửa form: không ghi đè bằng số liệu mới
let filledMib = '';   // giá trị MiB đã điền, để biết người dùng có sửa dung lượng không
let shards = null;    // danh sách hash lần tải gần nhất

const refresh = poll(load, 3000);

async function load() {
  let st;
  try {
    st = await get('api/status');
  } catch (err) {
    render($('#top-status'), html`<span class="pill bad">Mất kết nối tới node</span>
      <span class="muted">${err.message}</span>`);
    return;
  }
  $('#node-id').textContent = st.node_id;
  document.title = `dstore · ${st.node_id}`;
  render($('#top-status'), html`${statePill(st.connection)}
    <span class="muted">cập nhật ${new Date().toLocaleTimeString('vi-VN')}</span>`);
  renderBanners(st.connection);
  renderCards(st);
  renderConnection(st.connection);
  renderInfo(st);
  renderSettings(st);
  if (shards === null) loadShards();
}

const STATES = {
  connected: ['ok', 'Đã kết nối'],
  connecting: ['warn', 'Đang kết nối'],
  paused: ['bad', 'Đã ngắt kết nối'],
};

function statePill(c) {
  const [tone, label] = STATES[c.state] ?? ['', c.state];
  return html`<span class="pill ${tone}">${label}</span>`;
}

const banner = (kind, title, body) =>
  html`<div class="banner ${kind}" role="alert"><div><strong>${title}</strong> ${body}</div></div>`;

function renderBanners(c) {
  const out = [];
  if (c.state === 'paused') {
    out.push(banner('warn', 'Heartbeat đang ngắt.',
      'Coordinator sẽ coi node này là chết sau heartbeat timeout và chép shard của nó sang node khác.'));
  } else if (c.state === 'connecting' && c.failures > 0) {
    out.push(banner('bad', 'Không kết nối được coordinator.',
      `Đã thử ${c.failures} lần, vẫn đang thử lại. Lỗi gần nhất: ${c.last_error}`));
  }
  render($('#banners'), out);
}

function renderCards(st) {
  const c = st.connection;
  render($('#cards'), html`
    <div class="card">
      <div class="label">Kết nối</div>
      <div class="value small">${statePill(c)}</div>
      <div class="sub">coordinator trả lời lần cuối: ${ago(c.last_ok)}</div>
    </div>
    <div class="card">
      <div class="label">Coordinator</div>
      <div class="value small mono">${st.settings.coordinator_url}</div>
      <div class="sub">heartbeat mỗi ${st.settings.heartbeat_interval}</div>
    </div>
    <div class="card">
      <div class="label">Dung lượng đã dùng</div>
      <div class="value small">${bytes(st.used_bytes)} / ${bytes(st.settings.capacity)}</div>
      ${meter(st.used_bytes, st.settings.capacity)}
    </div>
    <div class="card">
      <div class="label">Shard</div>
      <div class="value">${st.shard_count}</div>
      <div class="sub mono">${st.storage}</div>
    </div>`);
}

function renderConnection(c) {
  const lastErr = c.last_error
    ? html`<span class="tone-bad">${c.last_error}</span> <span class="muted">(${ago(c.last_error_at)})</span>`
    : html`<span class="muted">không có</span>`;
  render($('#conn'), html`
    <dt>Trạng thái</dt><dd>${statePill(c)}</dd>
    <dt>Trả lời lần cuối</dt><dd>${ago(c.last_ok)}</dd>
    <dt>Lỗi liên tiếp</dt><dd>${c.failures}</dd>
    <dt>Lỗi gần nhất</dt><dd>${lastErr}</dd>`);
  $('#disconnect').disabled = c.state === 'paused';
  $('#connect').disabled = c.state !== 'paused';
}

function renderInfo(st) {
  const started = when(st.started_at);
  render($('#info'), html`
    <dt>Node ID</dt><dd class="mono">${st.node_id}</dd>
    <dt>Nơi lưu shard</dt><dd class="mono">${st.storage}</dd>
    <dt>File cấu hình</dt><dd class="mono">${st.config_file || html`<span class="muted">không có</span>`}</dd>
    <dt>Khởi động</dt><dd>${ago(st.started_at)}${started ? ` (${started.toLocaleString('vi-VN')})` : ''}</dd>`);
}

// --- Cấu hình kết nối ---

const form = $('#settings');
const field = (name) => form.elements.namedItem(name);

function renderSettings(st) {
  applied = st.settings;
  $('#persist-note').textContent = st.config_file
    ? `Lưu vào ${st.config_file}: restart vẫn giữ, và được dùng thay cho biến môi trường.`
    : 'Node chạy không có CONFIG_FILE: cấu hình chỉ có hiệu lực tới khi restart.';
  if (!dirty) fillForm(applied);
}

function fillForm(s) {
  field('coordinator_url').value = s.coordinator_url;
  field('addr').value = s.addr;
  filledMib = String(+(s.capacity / MiB).toFixed(2));
  field('capacity_mib').value = filledMib;
  field('heartbeat_interval').value = s.heartbeat_interval;
  setDirty(false);
}

function setDirty(d) {
  dirty = d;
  $('#settings-state').textContent = d ? 'có thay đổi chưa lưu' : '';
}

form.addEventListener('input', () => setDirty(true));
$('#settings-reset').addEventListener('click', () => {
  if (applied) fillForm(applied);
  render($('#test-result'), '');
});

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const mib = field('capacity_mib').value;
  const body = {
    coordinator_url: field('coordinator_url').value.trim(),
    addr: field('addr').value.trim(),
    // Không sửa ô dung lượng thì gửi lại đúng số byte cũ, tránh sai số làm tròn MiB.
    capacity: mib === filledMib ? applied.capacity : Math.round(parseFloat(mib) * MiB),
    heartbeat_interval: field('heartbeat_interval').value.trim(),
  };
  await busy(e.submitter, async () => {
    try {
      const res = await api('PUT', 'api/settings', body);
      setDirty(false);
      toast(res.saved ? 'Đã lưu, node đang đăng ký lại với coordinator' : 'Đã áp dụng, nhưng sẽ mất khi restart');
      refresh();
    } catch (err) {
      toast(err.message, 'bad');
    }
  });
});

$('#test-conn').addEventListener('click', async (e) => {
  const url = field('coordinator_url').value.trim();
  render($('#test-result'), html`<span class="muted">đang thử…</span>`);
  await busy(e.currentTarget, async () => {
    try {
      const r = await api('POST', 'api/test-connection', { coordinator_url: url });
      render($('#test-result'), r.ok
        ? html`<span class="pill ok">Kết nối được · ${r.latency_ms} ms · cụm có ${r.nodes} node</span>`
        : html`<span class="pill bad">Không kết nối được</span> <span class="tone-bad">${r.error}</span>`);
    } catch (err) {
      render($('#test-result'), html`<span class="pill bad">${err.message}</span>`);
    }
  });
});

// --- Ngắt / nối lại heartbeat ---

$('#disconnect').addEventListener('click', async (e) => {
  if (!confirm('Ngắt heartbeat tới coordinator?\nSau heartbeat timeout, coordinator sẽ chép shard của node này sang node khác.')) return;
  await busy(e.currentTarget, async () => {
    try {
      await api('POST', 'api/disconnect');
      toast('Đã ngắt heartbeat');
    } catch (err) {
      toast(err.message, 'bad');
    }
  });
  refresh();
});

$('#connect').addEventListener('click', async (e) => {
  await busy(e.currentTarget, async () => {
    try {
      await api('POST', 'api/connect');
      toast('Đang kết nối lại');
    } catch (err) {
      toast(err.message, 'bad');
    }
  });
  setTimeout(refresh, 500);
});

// --- Shard ---

async function loadShards() {
  try {
    shards = await get('../shards');
  } catch (err) {
    shards = [];
    toast(`Không liệt kê được shard: ${err.message}`, 'bad');
  }
  renderShards();
}

function renderShards() {
  const q = $('#shard-filter').value.trim().toLowerCase();
  const list = q ? shards.filter((h) => h.includes(q)) : shards;
  $('#shards-hint').textContent = `${list.length} shard${q ? ' khớp bộ lọc' : ''}`
    + (list.length > MAX_SHARDS ? `, hiện ${MAX_SHARDS} dòng đầu` : '')
    + '. Danh sách không tự làm mới vì với S3, liệt kê tốn một lượt gọi API mỗi 1000 shard.';
  render($('#shards'), list.length
    ? list.slice(0, MAX_SHARDS).map((h) => html`<li>${h}</li>`)
    : html`<li class="empty">Không có shard nào.</li>`);
}

$('#shard-filter').addEventListener('input', () => shards && renderShards());
$('#load-shards').addEventListener('click', (e) => busy(e.currentTarget, loadShards));
