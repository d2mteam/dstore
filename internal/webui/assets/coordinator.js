// Trang control plane. Số liệu tổng quan và cấu hình đọc từ API riêng của UI
// (api/...); danh sách node, file, manifest đọc thẳng từ API control plane
// (../nodes, ../files) — cùng API mà dsctl dùng.
import { $, api, get, html, render, bytes, ago, when, meter, durationMs, toast, busy, poll } from './common.js';

const MAX_ROWS = 200; // coordinator chưa phân trang, nên UI chỉ vẽ chừng này dòng

let applied = null;   // settings đang có hiệu lực trên server
let dirty = false;    // người dùng đang sửa form: không ghi đè bằng số liệu mới
// ID file đang mở phần chi tiết; nằm trên URL (#file=...) để gửi link được.
let openFile = new URLSearchParams(location.hash.slice(1)).get('file');
let scrollToDetail = openFile !== null; // mở trang từ link thì cuộn tới phần chi tiết

const refresh = poll(load, 3000);

async function load() {
  let overview, nodes, files;
  try {
    [overview, nodes, files] = await Promise.all([get('api/overview'), get('../nodes'), get('../files')]);
  } catch (err) {
    render($('#top-status'), html`<span class="pill bad">Mất kết nối tới coordinator</span>
      <span class="muted">${err.message}</span>`);
    return;
  }
  renderTop(overview);
  renderBanners(overview, nodes, files);
  renderCards(overview.stats, nodes, files);
  renderHealth(overview.stats, files);
  renderNodes(nodes, overview.settings);
  renderFiles(files);
  renderSettings(overview.settings);
  renderActivity(overview.counters, overview.startup);
  if (openFile) loadDetail(openFile, scrollToDetail);
  scrollToDetail = false;
}

// --- Tổng quan ---

function renderTop(o) {
  const db = o.database;
  let pill;
  if (!db.ok) pill = html`<span class="pill bad" title="${db.error}">DB metadata lỗi</span>`;
  else if (o.startup.database.startsWith('memory')) pill = html`<span class="pill warn">Metadata chỉ trong RAM</span>`;
  else pill = html`<span class="pill ok" title="${o.startup.database}">PostgreSQL · ${db.latency_ms} ms</span>`;
  render($('#top-status'), html`${pill}<span class="muted">cập nhật ${new Date().toLocaleTimeString('vi-VN')}</span>`);
}

const banner = (kind, title, body) =>
  html`<div class="banner ${kind}" role="alert"><div><strong>${title}</strong> ${body}</div></div>`;

function renderBanners(o, nodes, files) {
  const out = [];
  if (!o.database.ok) {
    out.push(banner('bad', 'Không kết nối được DB metadata.',
      `Upload và xóa file sẽ thất bại cho tới khi DB trả lời lại. Lỗi: ${o.database.error}`));
  }
  const damaged = files.filter((f) => f.status === 'damaged').length;
  if (damaged) {
    out.push(banner('bad', `${damaged} file đã mất dữ liệu.`,
      'Có segment còn dưới k shard khỏe, repair không dựng lại được.'));
  }
  const offline = nodes.filter((n) => n.status !== 'online').length;
  if (offline) {
    out.push(banner('warn', `${offline} node offline.`, o.settings.repair_paused
      ? 'Repair đang tạm dừng nên shard trên các node này chưa được chép lại.'
      : 'Repair đang chép lại shard của chúng sang node khác.'));
  }
  if (o.settings.repair_paused) {
    out.push(banner('warn', 'Repair tự động đang tạm dừng.',
      'Shard mất sẽ không được tái tạo cho tới khi bật lại. Cài đặt này được lưu, restart vẫn giữ.'));
  }
  if (o.settings.gc_paused) {
    out.push(banner('warn', 'GC tự động đang tạm dừng.', 'Shard mồ côi sẽ nằm lại trên node.'));
  }
  render($('#banners'), out);
}

function renderCards(st, nodes, files) {
  const total = st.nodes_online + st.nodes_offline;
  // Biên an toàn: segment yếu nhất còn mất thêm được bao nhiêu shard nữa.
  let margin = null;
  for (const f of files) margin = Math.min(margin ?? Infinity, f.min_healthy - f.k);
  const tone = margin === null ? '' : margin <= 0 ? 'tone-bad' : margin === 1 ? 'tone-warn' : 'tone-ok';

  render($('#cards'), html`
    <div class="card">
      <div class="label">Node online</div>
      <div class="value">${st.nodes_online}<span class="muted" style="font-size:15px"> / ${total}</span></div>
      <div class="sub">${st.nodes_offline ? `${st.nodes_offline} node offline` : 'tất cả đang chạy'}</div>
    </div>
    <div class="card">
      <div class="label">Dung lượng đã dùng</div>
      <div class="value small">${bytes(st.used_bytes)} / ${bytes(st.capacity_bytes)}</div>
      ${meter(st.used_bytes, st.capacity_bytes)}
    </div>
    <div class="card">
      <div class="label">File</div>
      <div class="value">${st.files}</div>
      <div class="sub">${st.segments} segment · ${st.shards_ok} shard khỏe${st.shards_lost ? ` · ${st.shards_lost} shard mất` : ''}</div>
    </div>
    <div class="card">
      <div class="label">Biên an toàn</div>
      <div class="value ${tone}">${margin === null ? '—' : margin < 0 ? 'Mất dữ liệu' : margin}</div>
      <div class="sub">${margin === null ? 'chưa có file nào'
        : margin < 0 ? 'có segment còn dưới k shard'
        : 'shard còn mất thêm được ở segment yếu nhất'}</div>
    </div>`);
}

function renderHealth(st, files) {
  const rows = Object.entries(st.segment_health)
    .map(([n, count]) => [Number(n), count])
    .sort((a, b) => b[0] - a[0]);
  if (!rows.length) {
    render($('#health'), html`<div class="empty">Chưa có segment nào.</div>`);
    return;
  }
  const full = Math.max(...files.map((f) => f.k + f.m));
  const kMax = Math.max(...files.map((f) => f.k));
  render($('#health'), rows.map(([n, count]) => {
    const tone = n >= full ? 'ok' : n > kMax ? 'warn' : 'bad';
    const width = (count / st.segments) * 100;
    return html`<div class="bar-row">
      <span>${n} shard khỏe</span>
      <div class="bar-track"><span class="${tone}" style="width:${width.toFixed(1)}%"></span></div>
      <span class="num">${count}</span>
    </div>`;
  }));
}

// --- Node ---

function renderNodes(nodes, settings) {
  $('#nodes-hint').textContent = `${nodes.length} node đã đăng ký`;
  if (!nodes.length) {
    render($('#nodes'), html`<tr><td colspan="6" class="empty">Chưa có node nào đăng ký.</td></tr>`);
    return;
  }
  const timeout = durationMs(settings.heartbeat_timeout);
  render($('#nodes'), nodes.map((n) => {
    const online = n.status === 'online';
    const age = Date.now() - (when(n.last_heartbeat)?.getTime() ?? 0);
    const late = online && age > (timeout * 2) / 3; // lỡ vài nhịp, sắp bị coi là chết
    const pill = !online ? html`<span class="pill bad">offline</span>`
      : late ? html`<span class="pill warn">chậm heartbeat</span>`
      : html`<span class="pill ok">online</span>`;
    return html`<tr>
      <td><strong>${n.id}</strong></td>
      <td class="mono">${n.addr}</td>
      <td>${pill}</td>
      <td>${bytes(n.used)} / ${bytes(n.capacity)}${meter(n.used, n.capacity)}</td>
      <td class="num">${n.shard_count}</td>
      <td class="muted">${ago(n.last_heartbeat)}</td>
    </tr>`;
  }));
}

// --- File ---

function renderFiles(files) {
  $('#files-hint').textContent = files.length > MAX_ROWS
    ? `hiện ${MAX_ROWS} / ${files.length} file mới nhất` : `${files.length} file`;
  if (!files.length) {
    render($('#files'), html`<tr><td colspan="7" class="empty">
      Chưa có file nào. Upload bằng <code>dsctl put</code> hoặc qua gateway.</td></tr>`);
    return;
  }
  render($('#files'), files.slice(0, MAX_ROWS).map((f) => {
    const total = f.k + f.m;
    const tone = f.min_healthy >= total ? 'ok' : f.min_healthy > f.k ? 'warn' : 'bad';
    return html`<tr class="${openFile === f.id ? 'selected' : ''}">
      <td>${f.name}</td>
      <td class="mono">${f.id}</td>
      <td class="num">${bytes(f.size)}</td>
      <td>${f.k} + ${f.m}</td>
      <td><span class="pill ${tone}" title="shard khỏe của segment yếu nhất">${f.min_healthy} / ${total}</span></td>
      <td class="muted">${ago(f.created_at)}</td>
      <td class="actions">
        <button class="btn small" type="button" data-detail="${f.id}">Chi tiết</button>
        <button class="btn small danger" type="button" data-delete="${f.id}" data-name="${f.name}">Xóa</button>
      </td>
    </tr>`;
  }));
}

$('#files').addEventListener('click', async (e) => {
  const btn = e.target.closest('button');
  if (!btn) return;
  if (btn.dataset.detail) {
    openFile = btn.dataset.detail;
    history.replaceState(null, '', `#file=${encodeURIComponent(openFile)}`);
    await loadDetail(openFile, true);
    refresh();
  } else if (btn.dataset.delete) {
    const { delete: id, name } = btn.dataset;
    if (!confirm(`Xóa file "${name}" (${id})?\nShard trên các node bị xóa theo, không khôi phục được.`)) return;
    await busy(btn, async () => {
      try {
        await api('DELETE', `../files/${encodeURIComponent(id)}`);
        if (openFile === id) closeDetail();
        toast(`Đã xóa ${name}`);
        refresh();
      } catch (err) {
        toast(err.message, 'bad');
      }
    });
  }
});

async function loadDetail(id, scroll) {
  let man;
  try {
    man = await get(`../files/${encodeURIComponent(id)}`);
  } catch (err) {
    closeDetail();
    toast(err.message, 'bad');
    return;
  }
  const total = man.k + man.m;
  const segs = man.segments.slice(0, MAX_ROWS);
  const keepScroll = $('#file-detail .shardgrid')?.scrollTop ?? 0;

  render($('#file-detail'), html`
    <div class="panel-head" style="margin-top:20px">
      <h2>${man.name} <span class="muted mono">${man.file_id}</span></h2>
      <button class="btn small" type="button" id="detail-close">Đóng</button>
    </div>
    <p class="hint">Mỗi hàng là một segment, mỗi ô là một shard kèm node đang giữ nó.
      Ô gạch ngang là shard mất vì node offline. Cần ít nhất ${man.k} ô xanh để đọc được segment.</p>
    <div class="shardgrid">${segs.map((seg) => {
      const byIndex = new Map(seg.shards.map((s) => [s.index, s]));
      const cells = [];
      for (let i = 0; i < total; i++) {
        const sh = byIndex.get(i);
        cells.push(sh
          ? html`<span class="shard ${sh.status === 'ok' ? '' : 'lost'}"
              title="shard ${i} · ${sh.node_id} · ${sh.status}\n${sh.hash}">${sh.node_id}</span>`
          : html`<span class="shard missing" title="shard ${i}: chưa có">—</span>`);
      }
      return html`<div class="seg-row"><span class="seg-label">seg ${seg.index}</span>${cells}</div>`;
    })}</div>
    ${man.segments.length > segs.length
      ? html`<p class="hint">… và ${man.segments.length - segs.length} segment nữa.</p>` : ''}`);

  $('#file-detail .shardgrid').scrollTop = keepScroll;
  $('#detail-close').onclick = () => { closeDetail(); refresh(); };
  if (scroll) $('#file-detail').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function closeDetail() {
  openFile = null;
  history.replaceState(null, '', location.pathname);
  render($('#file-detail'), '');
}

// --- Cấu hình vận hành ---

const form = $('#settings');
const field = (name) => form.elements.namedItem(name);

function renderSettings(s) {
  applied = s;
  if (!dirty) fillForm(s);
}

function fillForm(s) {
  field('heartbeat_timeout').value = s.heartbeat_timeout;
  field('gc_grace').value = s.gc_grace;
  field('repair_paused').checked = s.repair_paused;
  field('gc_paused').checked = s.gc_paused;
  setDirty(false);
}

function setDirty(d) {
  dirty = d;
  $('#settings-state').textContent = d ? 'có thay đổi chưa lưu' : '';
}

form.addEventListener('input', () => setDirty(true));
$('#settings-reset').addEventListener('click', () => applied && fillForm(applied));

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const body = {
    heartbeat_timeout: field('heartbeat_timeout').value.trim(),
    gc_grace: field('gc_grace').value.trim(),
    repair_paused: field('repair_paused').checked,
    gc_paused: field('gc_paused').checked,
  };
  await busy(e.submitter, async () => {
    try {
      await api('PUT', 'api/settings', body);
      setDirty(false);
      toast('Đã lưu cấu hình, có hiệu lực từ lượt chạy kế tiếp');
      refresh();
    } catch (err) {
      toast(err.message, 'bad');
    }
  });
});

// --- Hoạt động nền ---

const stat = (n, label) => html`<div class="stat"><div class="n">${n}</div><div class="l">${label}</div></div>`;

function renderActivity(c, st) {
  render($('#counters'), html`
    ${stat(c.repaired_shards, 'shard đã tái tạo')}
    ${stat(c.repair_failures, 'lượt repair lỗi')}
    ${stat(c.orphans_deleted, 'shard mồ côi đã dọn')}
    ${stat(c.nodes_lost, 'lần node mất heartbeat')}`);
  render($('#runs'), html`
    <dt>Repair lượt cuối</dt><dd>${ago(c.last_repair)} <span class="muted">· tự chạy mỗi ${st.repair_interval}</span></dd>
    <dt>GC lượt cuối</dt><dd>${ago(c.last_gc)} <span class="muted">· tự chạy mỗi ${st.gc_interval}</span></dd>`);
  const started = when(st.started_at);
  render($('#startup'), html`
    <dt>Nghe tại</dt><dd class="mono">${st.listen}</dd>
    <dt>DB metadata</dt><dd class="mono">${st.database}</dd>
    <dt>Quét heartbeat</dt><dd>mỗi ${st.health_interval}</dd>
    <dt>Khởi động</dt><dd>${ago(st.started_at)}${started ? ` (${started.toLocaleString('vi-VN')})` : ''}</dd>`);
}

async function trigger(btn, path, msg) {
  await busy(btn, async () => {
    try {
      await api('POST', path);
      toast(msg);
      setTimeout(refresh, 800);
    } catch (err) {
      toast(err.message, 'bad');
    }
  });
}

$('#run-repair').addEventListener('click', (e) => trigger(e.currentTarget, 'api/repair', 'Đã yêu cầu chạy repair'));
$('#run-gc').addEventListener('click', (e) => trigger(e.currentTarget, 'api/gc', 'Đã yêu cầu chạy GC'));
