const initUnsafe = (window.Telegram && window.Telegram.WebApp && window.Telegram.WebApp.initDataUnsafe) || {};
const authPayload = {
  auth_date: initUnsafe.auth_date,
  query_id: initUnsafe.query_id,
  user: initUnsafe.user,
  hash: initUnsafe.hash,
};

const headers = { 'X-Telegram-Auth': JSON.stringify(authPayload), 'Content-Type': 'application/json' };

function show(id) {
  document.querySelectorAll('section').forEach((s) => s.classList.remove('active'));
  document.getElementById(id).classList.add('active');
}

async function api(method, path, body) {
  const res = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}

async function loadDashboard() {
  const d = await api('GET', '/api/analytics/daily');
  document.getElementById('dashboardData').textContent = JSON.stringify(d, null, 2);
}

async function loadSources() {
  const d = await api('GET', '/api/sources');
  document.getElementById('sourcesData').textContent = JSON.stringify(d, null, 2);
}

async function addSource() {
  const channel = document.getElementById('sourceChannel').value.trim();
  if (!channel) return;
  await api('POST', '/api/sources', { channel });
  await loadSources();
}

async function loadMessages() {
  const d = await api('GET', '/api/messages');
  document.getElementById('messagesData').textContent = JSON.stringify(d.slice(0, 100), null, 2);
}

async function loadTags() {
  const d = await api('GET', '/api/tags');
  document.getElementById('tagsData').textContent = JSON.stringify(d, null, 2);
}

async function allowTag() {
  const name = document.getElementById('tagName').value.trim();
  await api('POST', '/api/tags/toggle', { name, is_allowed: true, is_blocked: false });
  await loadTags();
}

async function blockTag() {
  const name = document.getElementById('tagName').value.trim();
  await api('POST', '/api/tags/toggle', { name, is_allowed: false, is_blocked: true });
  await loadTags();
}

async function doSearch() {
  const query = document.getElementById('searchQuery').value.trim();
  const d = await api('POST', '/api/search', { query, limit: 20 });
  document.getElementById('searchData').textContent = JSON.stringify(d, null, 2);
}

async function loadSettings() {
  const d = await api('GET', '/api/settings');
  document.getElementById('settingsData').textContent = JSON.stringify(d, null, 2);
}

loadDashboard().catch(console.error);
loadSources().catch(console.error);
loadMessages().catch(console.error);
loadTags().catch(console.error);
