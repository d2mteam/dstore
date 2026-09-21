// Tiện ích dùng chung cho các trang quản trị: gọi API, định dạng số liệu, và
// dựng HTML an toàn — mọi giá trị chèn vào html`...` đều được escape, vì tên
// file hay ID node là dữ liệu người dùng gửi lên.

export const $ = (sel) => document.querySelector(sel);

// api gọi JSON và ném lỗi kèm thông điệp server trả về nếu mã khác 2xx.
export async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  // Mở trang bằng http://admin:mk@host/ui/ thì URL tương đối thừa hưởng phần
  // user:mk và fetch từ chối chạy; bỏ phần đó đi, trình duyệt vẫn tự gửi
  // thông tin đăng nhập đã nhớ.
  const url = new URL(path, location.href);
  url.username = '';
  url.password = '';
  const res = await fetch(url, opts);
  const text = await res.text();
  let data = null;
  if (text) {
    try { data = JSON.parse(text); } catch { data = text.trim(); }
  }
  if (!res.ok) {
    const msg = (data && data.error) || (typeof data === 'string' && data) || res.statusText;
    throw new Error(msg);
  }
  return data;
}

export const get = (path) => api('GET', path);

// --- HTML an toàn ---

class Html {
  constructor(s) { this.s = s; }
}

const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };

function piece(v) {
  if (v instanceof Html) return v.s;
  if (Array.isArray(v)) return v.map(piece).join('');
  if (v === null || v === undefined || v === false) return '';
  return String(v).replace(/[&<>"']/g, (c) => ESC[c]);
}

export function html(strings, ...vals) {
  let out = strings[0];
  vals.forEach((v, i) => { out += piece(v) + strings[i + 1]; });
  return new Html(out);
}

export function render(el, content) {
  el.innerHTML = piece(content);
}

// --- Định dạng ---

export function bytes(n) {
  if (!Number.isFinite(n)) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  let i = 0;
  while (Math.abs(n) >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(i === 0 || n >= 100 ? 0 : 1)} ${units[i]}`;
}

// Go trả thời điểm rỗng là "0001-01-01T00:00:00Z".
export function when(t) {
  if (!t) return null;
  const d = new Date(t);
  return Number.isNaN(d.getTime()) || d.getFullYear() < 2000 ? null : d;
}

export function ago(t) {
  const d = when(t);
  if (!d) return 'chưa có';
  const s = Math.max(0, Math.round((Date.now() - d.getTime()) / 1000));
  if (s < 2) return 'vừa xong';
  if (s < 60) return `${s} giây trước`;
  if (s < 3600) return `${Math.floor(s / 60)} phút trước`;
  if (s < 86400) return `${Math.floor(s / 3600)} giờ trước`;
  return `${Math.floor(s / 86400)} ngày trước`;
}

export function percent(part, whole) {
  return whole > 0 ? Math.min(100, Math.max(0, (part / whole) * 100)) : 0;
}

// "20s" -> 20000; dùng để so tuổi heartbeat với timeout.
export function durationMs(s) {
  const units = { ms: 1, s: 1e3, m: 6e4, h: 3.6e6 };
  let total = 0;
  for (const [, n, u] of String(s).matchAll(/([\d.]+)(ms|s|m|h)/g)) total += parseFloat(n) * units[u];
  return total;
}

export function meter(part, whole) {
  const p = percent(part, whole);
  const cls = p >= 90 ? 'bad' : p >= 75 ? 'warn' : '';
  return html`<div class="meter ${cls}"><span style="width:${p.toFixed(1)}%"></span></div>`;
}

// --- Thông báo ---

let toastTimer;
export function toast(msg, kind = '') {
  const el = $('#toast');
  el.textContent = msg;
  el.className = `toast ${kind}`;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.hidden = true; }, kind === 'bad' ? 6000 : 3000);
}

// busy khóa nút trong lúc chờ API, tránh bấm hai lần.
export async function busy(btn, fn) {
  btn.disabled = true;
  try { return await fn(); } finally { btn.disabled = false; }
}

// poll gọi fn ngay rồi lặp lại sau mỗi `ms`; tạm nghỉ khi tab bị ẩn.
export function poll(fn, ms) {
  let timer;
  const tick = async () => {
    if (!document.hidden) await fn();
    // Xóa hẹn giờ cũ ngay trước khi đặt mới: nếu hai lượt chạy chồng nhau
    // (bấm làm mới khi lượt tự động chưa xong) thì vẫn chỉ còn một vòng lặp.
    clearTimeout(timer);
    timer = setTimeout(tick, ms);
  };
  document.addEventListener('visibilitychange', () => { if (!document.hidden) tick(); });
  tick();
  return tick; // gọi lại để làm mới ngay, ví dụ sau khi bấm nút
}
