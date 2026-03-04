const tg = window.Telegram?.WebApp;
const init = tg?.initDataUnsafe || {};

const authPayload = {
  auth_date: init.auth_date,
  query_id: init.query_id,
  user: init.user,
  hash: init.hash,
};

const headers = {
  'X-Telegram-Auth': JSON.stringify(authPayload),
  'Content-Type': 'application/json',
};

const byId = (id) => document.getElementById(id);

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

function switchPage(pageId) {
  document.querySelectorAll('.page').forEach((p) => p.classList.remove('active'));
  document.querySelectorAll('.nav button').forEach((b) => b.classList.remove('active'));
  byId(pageId).classList.add('active');
  document.querySelector(`.nav button[data-page="${pageId}"]`)?.classList.add('active');
}

function boolChip(v) {
  return `<span class="chip ${v ? 'status-ok' : ''}">${v ? 'yes' : 'no'}</span>`;
}

async function loadDashboard() {
  const [daily, system, sources] = await Promise.all([
    api('GET', '/api/analytics/daily'),
    api('GET', '/health'),
    api('GET', '/api/sources'),
  ]);

  const total = daily.daily.reduce((acc, x) => acc + Number(x.total || 0), 0);
  const dup = daily.daily.reduce((acc, x) => acc + Number(x.duplicates || 0), 0);
  const pub = daily.daily.reduce((acc, x) => acc + Number(x.published || 0), 0);

  byId('mTotal').textContent = String(total);
  byId('mDup').textContent = String(dup);
  byId('mPub').textContent = String(pub);
  byId('mSrc').textContent = String(sources.length);
  byId('healthDump').textContent = JSON.stringify(system, null, 2);
}

async function loadSources() {
  const rows = await api('GET', '/api/sources');
  byId('sourcesBody').innerHTML = rows.map((r) => `
    <tr>
      <td>${r.id}</td>
      <td class="mono">${r.channel}</td>
      <td>${boolChip(r.is_active)}</td>
      <td>${r.priority}</td>
      <td>${r.total_messages}</td>
      <td>${r.published_messages}</td>
    </tr>
  `).join('');
}

async function addSource() {
  const channel = byId('sourceChannel').value.trim();
  const priority = Number(byId('sourcePriority').value || 100);
  if (!channel) return;
  await api('POST', '/api/sources', { channel, priority });
  byId('sourceChannel').value = '';
  await loadSources();
}

async function loadMessages() {
  const tag = byId('msgTag').value.trim();
  const query = tag ? `?tag=${encodeURIComponent(tag)}` : '';
  const rows = await api('GET', `/api/messages${query}`);
  byId('messagesBody').innerHTML = rows.map((m) => `
    <tr>
      <td>${m.id}</td>
      <td>${m.source_id}</td>
      <td class="mono">${new Date(m.created_at).toLocaleString()}</td>
      <td>${boolChip(m.is_duplicate)}</td>
      <td>${boolChip(m.is_published)}</td>
      <td>${String(m.text || '').slice(0, 180)}</td>
    </tr>
  `).join('');
}

async function loadTags() {
  const rows = await api('GET', '/api/tags');
  byId('tagsBody').innerHTML = rows.map((t) => `
    <tr>
      <td>${t.name}</td>
      <td>${boolChip(t.is_allowed)}</td>
      <td>${boolChip(t.is_blocked)}</td>
      <td>${t.usage_count}</td>
    </tr>
  `).join('');
}

async function setTag(allowed, blocked) {
  const name = byId('tagName').value.trim();
  if (!name) return;
  await api('POST', '/api/tags/toggle', { name, is_allowed: allowed, is_blocked: blocked });
  await loadTags();
}

async function runSearch() {
  const query = byId('searchQuery').value.trim();
  const limit = Number(byId('searchLimit').value || 20);
  if (!query) return;

  const rows = await api('POST', '/api/search', { query, limit });
  byId('searchBody').innerHTML = rows.map((r) => `
    <tr>
      <td>${r.id}</td>
      <td>${Number(r.similarity || 0).toFixed(4)}</td>
      <td class="mono">${new Date(r.created_at).toLocaleString()}</td>
      <td>${String(r.text || '').slice(0, 220)}</td>
    </tr>
  `).join('');
}

async function loadSettings() {
  const data = await api('GET', '/api/settings');
  byId('sBatch').value = data.batch_size ?? '';
  byId('sThreshold').value = data.dedupe_threshold ?? '';
  byId('sWindow').value = data.dedupe_window_days ?? '';
  byId('settingsDump').textContent = JSON.stringify(data, null, 2);
}

async function saveSettings() {
  const payload = {
    batch_size: Number(byId('sBatch').value || 0),
    dedupe_threshold: Number(byId('sThreshold').value || 0),
    dedupe_window_days: Number(byId('sWindow').value || 0),
  };
  await api('PATCH', '/api/settings', payload);
  await loadSettings();
}

function bindEvents() {
  document.querySelectorAll('.nav button').forEach((btn) => {
    btn.addEventListener('click', () => switchPage(btn.dataset.page));
  });

  byId('addSourceBtn').addEventListener('click', () => addSource().catch(console.error));
  byId('reloadSourcesBtn').addEventListener('click', () => loadSources().catch(console.error));
  byId('reloadMessagesBtn').addEventListener('click', () => loadMessages().catch(console.error));
  byId('allowTagBtn').addEventListener('click', () => setTag(true, false).catch(console.error));
  byId('blockTagBtn').addEventListener('click', () => setTag(false, true).catch(console.error));
  byId('reloadTagsBtn').addEventListener('click', () => loadTags().catch(console.error));
  byId('runSearchBtn').addEventListener('click', () => runSearch().catch(console.error));
  byId('saveSettingsBtn').addEventListener('click', () => saveSettings().catch(console.error));
  byId('reloadSettingsBtn').addEventListener('click', () => loadSettings().catch(console.error));
}

async function bootstrap() {
  bindEvents();
  await Promise.all([
    loadDashboard(),
    loadSources(),
    loadMessages(),
    loadTags(),
    loadSettings(),
  ]);
}

bootstrap().catch((err) => {
  console.error(err);
  byId('healthDump').textContent = String(err);
});
