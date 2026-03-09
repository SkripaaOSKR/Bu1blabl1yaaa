const tg = window.Telegram?.WebApp;
const initData = tg?.initDataUnsafe || {};

// Инициализация Telegram WebApp
if (tg) {
    tg.ready();
    tg.expand();
    // Устанавливаем цвет темы
    document.documentElement.style.setProperty('--tg-theme-bg-color', tg.backgroundColor);
}

// Подготовка заголовков для авторизации (X-Telegram-Auth)
const authPayload = {
    auth_date: initData.auth_date,
    query_id: initData.query_id,
    user: initData.user,
    hash: initData.hash,
};

const headers = {
    'X-Telegram-Auth': JSON.stringify(authPayload),
    'Content-Type': 'application/json',
};

const byId = (id) => document.getElementById(id);

// --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

// Вибрация (Haptic Feedback)
function haptic(type = 'light') {
    if (tg && tg.HapticFeedback) {
        if (type === 'success') tg.HapticFeedback.notificationOccurred('success');
        else if (type === 'error') tg.HapticFeedback.notificationOccurred('error');
        else tg.HapticFeedback.impactOccurred(type);
    }
}

// Универсальный вызов API с обработкой ошибок
async function api(method, path, body) {
    try {
        const res = await fetch(path, {
            method,
            headers,
            body: body ? JSON.stringify(body) : undefined,
        });
        if (!res.ok) {
            const errorText = await res.text();
            throw new Error(`${res.status}: ${errorText}`);
        }
        return await res.json();
    } catch (err) {
        console.error(`Ошибка API (${path}):`, err);
        haptic('error');
        if (tg) tg.showAlert(`Ошибка: ${err.message}`);
        return null;
    }
}

// --- ЛОГИКА ДАШБОРДА (АНАЛИТИКА) ---

async function loadDashboard() {
    const dailyData = await api('GET', '/api/analytics/daily');
    const healthData = await api('GET', '/health');

    if (dailyData) {
        // Общая статистика сверху
        if (dailyData.daily && dailyData.daily.length > 0) {
            const today = dailyData.daily[0];
            if (byId('mTotal')) byId('mTotal').textContent = today.total || 0;
            if (byId('mPub')) byId('mPub').textContent = today.published || 0;
        }

        // Дополнительная аналитика (Топ источников)
        let extraHtml = `<div style="margin-top: 10px;">`;
        
        if (dailyData.top_sources && dailyData.top_sources.length > 0) {
            extraHtml += `<div style="font-size: 13px; font-weight: 700; margin-bottom: 8px;">🔝 Топ источников:</div>`;
            dailyData.top_sources.slice(0, 5).forEach(s => {
                extraHtml += `<div style="font-size: 12px; margin-bottom: 4px; display: flex; justify-content: space-between;">
                    <span>${s.channel}</span>
                    <b style="color: var(--button-color)">${s.total_messages}</b>
                </div>`;
            });
        }
        extraHtml += `</div>`;
        
        // Вставляем красивую аналитику в блок статуса
        if (byId('healthDump')) {
            if (healthData) {
                const ws = healthData.worker_state || {};
                const dbStatus = healthData.database === 'ok' ? '🟢 OK' : '🔴 Ошибка';
                let cpDate = 'Нет данных';
                if (ws.last_checkpoint) {
                    const d = new Date(ws.last_checkpoint);
                    cpDate = d.toLocaleString([], {dateStyle: 'short', timeStyle: 'short'});
                }

                const stateHtml = `
<div style="font-size: 12px; line-height: 1.5;">
  <b>База данных:</b> ${dbStatus}
  <br><b>Векторов FAISS:</b> ${healthData.faiss_vectors_count || 0}
  <br><b>Активных каналов:</b> ${healthData.active_sources || 0}
  <hr style="border: none; border-top: 1px dashed rgba(0,0,0,0.1); margin: 8px 0;">
  <b>Последний цикл Сборщика:</b>
  <br>• Статус: ${ws.last_status === 'success' ? '✅ Успех' : (ws.last_status || 'N/A')}
  <br>• Задач в очередь: ${ws.last_count || 0}
  <br>• Чекпоинт: ${cpDate}
</div>`;
                byId('healthDump').innerHTML = stateHtml + extraHtml;
            } else {
                byId('healthDump').innerHTML = "Система работает, но детальная статистика недоступна.\n" + extraHtml;
            }
        }
    }
}

// Быстрый запуск воркера
async function quickRun() {
    haptic('medium');
    const res = await api('POST', '/api/processing/run', { hours: 24 });
    if (res) {
        haptic('success');
        if (tg) tg.showScanQrPopup({ text: "Сборщик запущен!" });
        setTimeout(() => tg && tg.closeScanQrPopup(), 1500);
        loadDashboard();
    }
}

// --- ЛОГИКА КАНАЛОВ (SOURCES) ---

let allSources = []; 

async function loadSources() {
    const rows = await api('GET', '/api/sources');
    if (!rows) return;
    allSources = rows;
    renderSources(allSources);
}

function renderSources(list) {
    if (!byId('sourcesList')) return;
    byId('sourcesList').innerHTML = list.map((r) => `
        <div class="card">
            <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 10px;">
                <div style="max-width: 70%;">
                    <div class="mono" style="font-weight: 800; font-size: 15px; color: var(--button-color);">
                        ${r.title ? r.title : r.channel}
                    </div>
                    <div style="font-size: 11px; color: var(--hint-color); margin-top: 4px;">
                        ${r.title ? `Ссылка: ${r.channel} <br>` : ''}
                        ID: ${r.id} | Ветка: <span class="chip">${r.topic_id || 'Общая'}</span>
                    </div>
                </div>
                <span class="chip ${r.is_active ? 'chip-success' : ''}">${r.is_active ? 'Активен' : 'Выкл'}</span>
            </div>
            <div class="grid-2" style="margin-bottom: 12px;">
                <div style="font-size: 12px;">📥 Собрано: <b>${r.total_messages}</b></div>
                <div style="font-size: 12px;">✅ Постов: <b>${r.published_messages}</b></div>
            </div>
            <div style="display: flex; gap: 8px;">
                <button class="btn btn-secondary btn-small" style="flex: 1;" onclick="toggleSource(${r.id}, ${!r.is_active})">
                    ${r.is_active ? 'Выключить' : 'Включить'}
                </button>
                <button class="btn btn-danger btn-small" style="width: 40px;" onclick="deleteSource(${r.id})">🗑</button>
            </div>
        </div>
    `).join('');
}

// Поиск по списку источников
if (byId('searchSources')) {
    byId('searchSources').addEventListener('input', (e) => {
        const val = e.target.value.toLowerCase();
        const filtered = allSources.filter(s => s.channel.toLowerCase().includes(val));
        renderSources(filtered);
    });
}

async function saveSource() {
    haptic('light');
    const channel = byId('sourceChannel').value.trim();
	const title = byId('sourceTitle').value.trim() || null;
    const topic_id = byId('sourceTopic').value.trim() || null;
    
    if (!channel) return;

    const res = await api('POST', '/api/sources', { 
        channel, 
		title,
        topic_id: topic_id ? parseInt(topic_id) : null 
    });

    if (res) {
        haptic('success');
        byId('sourceChannel').value = '';
        byId('sourceTopic').value = '';
        closeModal('addSourceModal');
        loadSources();
    }
}

window.deleteSource = async (id) => {
    if (!confirm("Удалить этот канал и все его сообщения?")) return;
    haptic('medium');
    const res = await api('DELETE', `/api/sources/${id}`);
    if (res) { haptic('success'); loadSources(); }
};

window.toggleSource = async (id, active) => {
    haptic('light');
    const res = await api('PATCH', `/api/sources/${id}`, { is_active: active });
    if (res) { haptic('success'); loadSources(); }
};

// --- ЛОГИКА ФИЛЬТРОВ (KEYWORDS - ИМБА ФИЧА) ---

async function loadKeywords() {
    const rows = await api('GET', '/api/spam_keywords');
    if (!rows || !byId('keywordsList')) return;
    
    byId('keywordsList').innerHTML = rows.map((k) => `
        <div class="card" style="display: flex; justify-content: space-between; align-items: center; padding: 12px 16px;">
            <div>
                <span style="font-weight: 600; font-size: 16px; text-decoration: ${k.is_active ? 'none' : 'line-through'}; opacity: ${k.is_active ? '1' : '0.5'};">${k.word}</span>
                <br><small style="color: var(--hint-color)">ID: ${k.id}</small>
            </div>
            <div style="display: flex; gap: 8px; align-items: center;">
                <button class="btn btn-secondary btn-small" style="width: 44px;" onclick="toggleKeyword(${k.id})">
                    ${k.is_active ? '✅' : '⚪'}
                </button>
                <button class="btn btn-danger btn-small" style="width: 44px;" onclick="deleteKeyword(${k.id})">🗑</button>
            </div>
        </div>
    `).join('');
}

async function addKeyword() {
    haptic('light');
    const word = byId('newKeyword').value.trim();
    if (!word) return;
    const res = await api('POST', '/api/spam_keywords', { word });
    if (res) {
        haptic('success');
        byId('newKeyword').value = '';
        loadKeywords();
    }
}

window.toggleKeyword = async (id) => {
    haptic('light');
    const res = await api('PATCH', `/api/spam_keywords/${id}/toggle`);
    if (res) { haptic('success'); loadKeywords(); }
};

window.deleteKeyword = async (id) => {
    haptic('medium');
    const res = await api('DELETE', `/api/spam_keywords/${id}`);
    if (res) { haptic('success'); loadKeywords(); }
};

// --- ЛОГИКА ПОСТОВ (MESSAGES + REPUBLISH) ---

async function loadMessages() {
    if (!byId('msgTag') || !byId('messagesList')) return;
    const tag = byId('msgTag').value.trim();
    const query = tag ? `?tag=${encodeURIComponent(tag)}` : '';
    const rows = await api('GET', `/api/messages${query}`);
    if (!rows) return;

    byId('messagesList').innerHTML = rows.map((m) => `
        <div class="card">
            <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                <span class="chip mono">${new Date(m.created_at).toLocaleString([], {dateStyle: 'short', timeStyle: 'short'})}</span>
                ${m.is_published ? 
                    '<span class="chip chip-success">Опубликован</span>' : 
                    (m.is_duplicate ? '<span class="chip" style="color: var(--danger-color)">Дубликат</span>' : '<span class="chip chip-warn">В Карантине</span>')
                }
            </div>
            <div style="font-size: 14px; line-height: 1.4; color: var(--text-color); margin-bottom: 12px; max-height: 150px; overflow-y: auto;">
                ${m.text ? m.text : '<i>[Медиа без текста]</i>'}
            </div>
            <div style="display: flex; gap: 8px;">
                ${!m.is_published ? `
                    <button class="btn btn-success btn-small" style="flex: 1;" onclick="republishMessage(${m.id})">✅ В паблик</button>
                    <button class="btn btn-danger btn-small" style="flex: 1;" onclick="markAsSpam(${m.id})">🚫 В спам</button>
                ` : `
                    <button class="btn btn-danger btn-small" style="width: 100%;" onclick="markAsSpam(${m.id})">🚫 Пометить как спам (обучить)</button>
                `}
            </div>
        </div>
    `).join('');
}

window.republishMessage = async (id) => {
    haptic('medium');
    const res = await api('POST', `/api/messages/${id}/republish`);
    if (res) {
        haptic('success');
        loadMessages();
        loadDashboard();
    }
};

window.deleteMessage = async (id) => {
    if (!confirm("Удалить это сообщение из базы?")) return;
    haptic('light');
    const res = await api('DELETE', `/api/messages/${id}`);
    if (res) {
        haptic('success');
        loadMessages();
    }
};

// НОВЫЙ ФУНКЦИОНАЛ: Пометить как спам
window.markAsSpam = async (id) => {
    if (!confirm("Удалить этот пост и обучить фильтр считать его спамом?")) return;
    haptic('medium');
    // На бэкенде удаление сообщения также триггерит функцию mark_confirmed_spam
    const res = await api('DELETE', `/api/messages/${id}`);
    if (res) {
        haptic('success');
        loadMessages();
    }
};

// --- НАСТРОЙКИ И СИСТЕМА ---

async function loadSettings() {
    const data = await api('GET', '/api/settings');
    if (data) {
        if (byId('sThreshold')) byId('sThreshold').value = data.dedupe_threshold || 0.8;
        if (byId('sBatch')) byId('sBatch').value = data.batch_size || 200;
        if (byId('sWindow')) byId('sWindow').value = data.dedupe_window_days || 14;
        if (byId('sMaxChars')) byId('sMaxChars').value = data.max_merge_chars || 1800;
        if (byId('sPrompt')) byId('sPrompt').value = data.ai_prompt || "";
    }
}

async function saveSettings() {
    haptic('medium');
    const payload = {};
    
    const threshold = parseFloat(byId('sThreshold')?.value);
    const batch = parseInt(byId('sBatch')?.value);
    const windowDays = parseInt(byId('sWindow')?.value);
    const maxChars = parseInt(byId('sMaxChars')?.value);
    const prompt = byId('sPrompt')?.value;

    if (!isNaN(threshold)) payload.dedupe_threshold = threshold;
    if (!isNaN(batch)) payload.batch_size = batch;
    if (!isNaN(windowDays)) payload.dedupe_window_days = windowDays;
    if (!isNaN(maxChars)) payload.max_merge_chars = maxChars;
    if (prompt !== undefined) payload.ai_prompt = prompt;

    const res = await api('PATCH', '/api/settings', payload);
    if (res) {
        haptic('success');
        if (tg) tg.showAlert("Настройки успешно сохранены!");
    }
}

// Очистка и пересборка индекса FAISS
async function clearCache() {
    if (!confirm("Это пересоберет векторный индекс из базы данных. Продолжить?")) return;
    haptic('error');
    await api('GET', '/health');
    if (tg) tg.showAlert("Команда на синхронизацию отправлена.");
}

// --- ИНИЦИАЛИЗАЦИЯ ---

function init() {
    // Привязка событий кнопкам (с проверкой на наличие элемента на странице)
    if (byId('quickRunBtn')) byId('quickRunBtn').addEventListener('click', quickRun);
    if (byId('saveSourceBtn')) byId('saveSourceBtn').addEventListener('click', saveSource);
    if (byId('addKeywordBtn')) byId('addKeywordBtn').addEventListener('click', addKeyword);
    if (byId('reloadMessagesBtn')) byId('reloadMessagesBtn').addEventListener('click', () => { haptic('light'); loadMessages(); });
    if (byId('saveSettingsBtn')) byId('saveSettingsBtn').addEventListener('click', saveSettings);
    if (byId('clearCacheBtn')) byId('clearCacheBtn').addEventListener('click', clearCache);

    // Первичная загрузка всех данных
    loadDashboard();
    loadSources();
    loadKeywords();
    loadMessages();
    loadSettings();
}

// Запуск приложения
init();