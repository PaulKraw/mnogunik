/**
 * panel3.js — Панель 3: Лендинг + CRM
 * Вся интерактивность: формы, прогресс-бар, аккордеон, авторизация, трекер клиентов
 */

const Panel3 = (() => {

  // ═══════════════════════════════════════════
  // MOCK DATA
  // ═══════════════════════════════════════════

  const MOCK_CLIENTS = [
    {
      key: 'svai_evg',
      name: 'ООО «СвайМонтаж»',
      category: 'Винтовые сваи',
      priority: 'bad',
      cpl: 480,
      delta: +12,
      stats: { ads: 1240, spend7d: 45600, contacts7d: 95, dynamicPct: -8 },
      stages: [
        { done: true, note: 'Прайс получен 12.03, 340 позиций. Структура хорошая.' },
        { done: true, note: '3 гипотезы: акцент на цену, на скорость монтажа, на гарантию 50 лет.' },
        { done: true, note: 'Выгрузка на FTP 15.03. 1240 объявлений активно.' },
        { done: true, note: 'Скачивание настроено, последний запуск вчера.' },
        { done: true, note: 'CTR гипотезы «цена» — 2.8%, остальные ниже 1.5%.' },
        { done: false, note: 'Нужно поднять ставки на гипотезу «цена», отключить «гарантию».' },
        { done: false, note: '' },
        { done: false, note: '' },
      ]
    },
    {
      key: 'zabor_srg',
      name: 'ИП Сергеев А.В.',
      category: 'Заборы и ограждения',
      priority: 'ok',
      cpl: 280,
      delta: -3,
      stats: { ads: 860, spend7d: 22400, contacts7d: 80, dynamicPct: +5 },
      stages: [
        { done: true, note: 'Excel-файл, 180 позиций штакетника + 40 ворот.' },
        { done: true, note: '2 гипотезы: штакетник-красивый и штакетник-дешёвый.' },
        { done: true, note: 'XML загружен. Обработка прошла без ошибок.' },
        { done: true, note: '' },
        { done: true, note: 'Обе гипотезы работают примерно одинаково.' },
        { done: true, note: 'Ставки на автомате, CPL стабильный.' },
        { done: false, note: 'Клиент просит расширить на Москву.' },
        { done: false, note: '' },
      ]
    },
    {
      key: 'sborpk_ozon',
      name: 'ULTRAFPS',
      category: 'Сборка ПК',
      priority: 'good',
      cpl: 120,
      delta: -22,
      stats: { ads: 4200, spend7d: 18900, contacts7d: 158, dynamicPct: +18 },
      stages: [
        { done: true, note: 'Прайс из pricecraft. 4200 конфигураций.' },
        { done: true, note: 'Авито + Ozon + WB. Разные шаблоны под каждую площадку.' },
        { done: true, note: 'Авито — XML. Ozon — API. WB — файл.' },
        { done: true, note: 'Авито + Ozon автоматически. WB вручную.' },
        { done: true, note: 'Авито CPL 120₽, Ozon CPL 95₽.' },
        { done: true, note: 'Формула работает хорошо, не трогаем.' },
        { done: true, note: 'Масштабировали с 2000 до 4200 конфигов.' },
        { done: true, note: 'Антиблокировка работает, 12 пересозданий за неделю.' },
      ]
    },
    {
      key: 'dezi_pch',
      name: 'ООО «ЧистоДом»',
      category: 'Дезинфекция',
      priority: 'ok',
      cpl: 340,
      delta: 0,
      stats: { ads: 320, spend7d: 8500, contacts7d: 25, dynamicPct: 0 },
      stages: [
        { done: true, note: '' },
        { done: true, note: 'Услуги — отдельное объявление на каждый город.' },
        { done: true, note: 'Через API, 15 городов.' },
        { done: true, note: '' },
        { done: false, note: 'Ждём неделю данных.' },
        { done: false, note: '' },
        { done: false, note: '' },
        { done: false, note: '' },
      ]
    }
  ];

  const STAGE_NAMES = [
    'Подготовка прайса',
    'Маркетинговые гипотезы',
    'Публикация объявлений',
    'Сбор статистики',
    'Анализ результатов',
    'Оптимизация ставок',
    'Масштабирование',
    'Антиблокировка'
  ];

  const STEPS_DATA = [
    { emoji: '📋', label: 'Этап 1 · Данные', title: 'Подготовка прайса', short: 'Получение и нормализация данных клиента в единый формат',
      desc: 'Клиент присылает прайс-лист в любом виде: Excel, Google Sheets, Word, даже фото. Специалист приводит данные к единому формату, где каждая строка = одна позиция (SKU) с фиксированными параметрами: название, цена, характеристики.',
      flow: [['📄 Прайс клиента', 'input'], ['→', 'arrow'], ['🔧 Нормализация', 'tool'], ['→', 'arrow'], ['📊 catalog.csv', 'output']]
    },
    { emoji: '💡', label: 'Этап 2 · Контент', title: 'Маркетинговые гипотезы', short: 'Создание нескольких вариантов объявлений для A/B-теста',
      desc: 'Одного текста недостаточно — создаётся 2–5 вариантов (гипотез): разные заголовки, описания, картинки. Claude API генерирует уникальные тексты, Pillow создаёт картинки из шаблонов. У каждого объявления — уникальные поля для защиты от блокировки.',
      flow: [['📝 Шаблоны', 'input'], ['→', 'arrow'], ['🤖 Claude API + Pillow', 'tool'], ['→', 'arrow'], ['📄 variants.csv', 'output']]
    },
    { emoji: '🚀', label: 'Этап 3 · Размещение', title: 'Публикация объявлений', short: 'Два пути: API (услуги) и XML/FTP (товары)',
      desc: 'Услуги публикуются через API Авито — каждое объявление отправляется отдельным запросом. Товары — через XML-файл на FTP (до 50 000 позиций). Авито обрабатывает файл раз в час. Для каждого объявления получаем avito_id.',
      flow: [['📄 variants.csv', 'input'], ['→', 'arrow'], ['🔌 API / FTP', 'tool'], ['→', 'arrow'], ['💾 avito_id в БД', 'output']]
    },
    { emoji: '📊', label: 'Этап 4 · Метрики', title: 'Сбор статистики', short: 'Автоматическое скачивание показов, просмотров, контактов',
      desc: 'Скрипт download.py запрашивает API Авито каждый день и сохраняет метрики: показы, просмотры, контакты, избранное, расход. Данные хранятся в SQLite с разбивкой по дням.',
      flow: [['🔌 Avito Stats API', 'input'], ['→', 'arrow'], ['💾 download.py', 'tool'], ['→', 'arrow'], ['📊 item_stats', 'output']]
    },
    { emoji: '🔍', label: 'Этап 5 · Аналитика', title: 'Анализ результатов', short: 'Расчёт CTR, CPL, сравнение гипотез, выявление лидеров',
      desc: 'build_stats.py агрегирует данные за 7 дней и предыдущие 7 дней. Рассчитывает CTR (конверсия показ→просмотр), CVR (просмотр→контакт), CPL (цена лида), CPV (цена просмотра). Сравнивает с предыдущим периодом.',
      flow: [['📊 item_stats', 'input'], ['→', 'arrow'], ['⚡ build_stats.py', 'tool'], ['→', 'arrow'], ['📈 current_stats', 'output']]
    },
    { emoji: '⚙️', label: 'Этап 6 · Управление', title: 'Оптимизация ставок', short: 'Автоматический расчёт и применение ставок по формуле',
      desc: 'export_stats.py выгружает аналитику в Google Sheets, читает мин/макс ставки, пересчитывает по формуле. apply_bids.py отправляет рассчитанные ставки на Авито через API setManual.',
      flow: [['📈 current_stats', 'input'], ['→', 'arrow'], ['📋 Google Sheets', 'tool'], ['→', 'arrow'], ['✅ Ставки на Авито', 'output']]
    },
    { emoji: '📈', label: 'Этап 7 · Рост', title: 'Масштабирование', short: 'Победившую гипотезу применяем ко всем похожим SKU',
      desc: 'Когда нашли формулу, которая работает (хороший CTR + низкий CPL) — берём тот же шаблон и применяем ко всем похожим позициям. Расширяем географию, увеличиваем количество объявлений.',
      flow: [['✅ Лучшая гипотеза', 'input'], ['→', 'arrow'], ['🔄 Клонирование', 'tool'], ['→', 'arrow'], ['📈 ×10 объявлений', 'output']]
    },
    { emoji: '🛡️', label: 'Этап 8 · Защита', title: 'Антиблокировка', short: 'Автоматическое пересоздание заблокированных объявлений',
      desc: 'При блокировке — система автоматически создаёт новое объявление с изменёнными уникальными полями (артикул, вес, высота). Новый avito_id привязывается к тому же внутреннему ID — статистика не теряется.',
      flow: [['🚫 Блокировка', 'input'], ['→', 'arrow'], ['🔄 Пересоздание', 'tool'], ['→', 'arrow'], ['✅ Новый avito_id', 'output']]
    }
  ];

  const DB_TABLES = [
    { name: 'items', desc: 'Справочник объявлений', status: 'done', group: 'stats' },
    { name: 'item_stats', desc: 'Статистика по дням', status: 'done', group: 'stats' },
    { name: 'current_stats', desc: 'Сводная аналитика 7д', status: 'done', group: 'stats' },
    { name: 'bids_history', desc: 'История ставок', status: 'done', group: 'stats' },
    { name: 'sync_log', desc: 'Лог операций', status: 'done', group: 'stats' },
    { name: 'hypothesis_stats', desc: 'Метрики гипотез', status: 'new', group: 'stats' },
    { name: 'leads', desc: 'Заявки с форм', status: 'done', group: 'crm' },
    { name: 'lead_stages', desc: 'Статусы этапов', status: 'done', group: 'crm' },
    { name: 'lead_stage_notes', desc: 'Заметки по этапам', status: 'done', group: 'crm' },
    { name: 'clients', desc: 'Список клиентов', status: 'new', group: 'crm' },
    { name: 'accounts', desc: 'Аккаунты Авито', status: 'new', group: 'gen' },
    { name: 'city_groups', desc: 'Группы городов', status: 'new', group: 'gen' },
    { name: 'hypotheses', desc: 'Таблица гипотез', status: 'new', group: 'gen' },
    { name: 'hypothesis_vars', desc: 'Переменные гипотез', status: 'new', group: 'gen' },
    { name: 'generation_tasks', desc: 'Задачи на генерацию', status: 'new', group: 'gen' },
    { name: 'templates_text', desc: 'Шаблоны текстов', status: 'new', group: 'gen' },
    { name: 'templates_img', desc: 'Шаблоны картинок', status: 'new', group: 'gen' },
    { name: 'blocked_ads', desc: 'Заблокированные', status: 'new', group: 'stats' },
    { name: 'ad_links', desc: 'Связи old→new ID', status: 'new', group: 'stats' },
    { name: 'bid_formulas', desc: 'Формулы ставок', status: 'new', group: 'stats' },
    { name: 'account_limits', desc: 'Лимиты аккаунтов', status: 'new', group: 'gen' },
  ];

  const TECH_STACK = [
    { icon: '🐍', name: 'Python 3.8+', use: 'Генерация, аналитика, API-интеграции' },
    { icon: '🖼️', name: 'Pillow (PIL)', use: 'Генерация и уникализация картинок' },
    { icon: '📊', name: 'pandas', use: 'Работа с DataFrame, прайсами' },
    { icon: '💾', name: 'SQLite', use: 'Хранение статистики и аналитики' },
    { icon: '📋', name: 'Google Sheets', use: 'Прайсы, ставки, задачи' },
    { icon: '🔌', name: 'Avito API', use: 'Публикация, статистика, ставки' },
    { icon: '🐘', name: 'PHP', use: 'Запуск скриптов, веб-эндпоинты' },
    { icon: '🌐', name: 'HTML / JS', use: 'Фронтенд панелей управления' },
    { icon: '⚡', name: 'nginx', use: 'Веб-сервер, статика, FTP' },
  ];

  const RESULTS = [
    { icon: '📈', title: 'Поток заявок', desc: 'Десятки контактов в неделю от целевой аудитории через Авито' },
    { icon: '⚡', title: 'Скорость запуска', desc: '3000 объявлений за 30 минут вместо недель ручной работы' },
    { icon: '🎯', title: 'Тестирование', desc: 'A/B-тесты гипотез — находим то, что работает для вашей ниши' },
    { icon: '💰', title: 'Экономия', desc: 'Автоматическое управление ставками снижает CPL на 20–40%' },
    { icon: '🛡️', title: 'Защита', desc: 'Антиблокировка: объявления пересоздаются автоматически' },
    { icon: '📊', title: 'Прозрачность', desc: 'Еженедельные метрики: расход, контакты, CTR, CPL — всё видно' },
  ];

  // ═══════════════════════════════════════════
  // AUTH STATE
  // ═══════════════════════════════════════════

  const AUTH_PASSWORD = 'demo2024';
  let loginAttempts = 0;
  let lockUntil = 0;
  let lockInterval = null;
  let expandedClient = null;

  // ═══════════════════════════════════════════
  // INIT
  // ═══════════════════════════════════════════

  function init() {
    // renderSteps();
    renderDbTables();
    // renderTechStack();
    // renderResults();
    updateProgress();
  }

  // ═══════════════════════════════════════════
  // PROGRESS BAR
  // ═══════════════════════════════════════════

  function updateProgress() {
    const done = document.querySelectorAll('.step-checkbox.done').length;
    const pct = Math.round(done / 8 * 100);
    document.getElementById('progressFill').style.width = pct + '%';
    document.getElementById('progressPct').textContent = done + ' из 8';
  }


  // ═══════════════════════════════════════════
  // STEPS ACCORDION
  // ═══════════════════════════════════════════

  function renderSteps() {
    const container = document.getElementById('flowList');
    let html = '';

    STEPS_DATA.forEach((s, i) => {
      if (i > 0) html += '<div class="step-connector"><div class="step-connector-line"></div></div>';

      const flowHtml = s.flow.map(f => {
        if (f[1] === 'arrow') return '<span class="df-arrow">→</span>';
        return `<span class="df-item ${f[1]}">${f[0]}</span>`;
      }).join('');

      html += `
        <div class="step-card" id="step-${i+1}" style="animation-delay: ${i * 0.05}s">
          <div class="step-header" onclick="Panel3.toggleStep(${i+1})">
            <div class="step-num-wrap"><span class="step-num">${String(i+1).padStart(2,'0')}</span></div>
            <div class="step-emoji">${s.emoji}</div>
            <div class="step-head-body">
              <div class="step-label">${s.label}</div>
              <div class="step-title">${s.title}</div>
              <div class="step-short">${s.short}</div>
            </div>
            <div class="step-check-area">
              <div class="step-checkbox" onclick="Panel3.toggleCheck(event, ${i+1})" title="Отметить выполненным"></div>
              <div class="chevron">▼</div>
            </div>
          </div>
          <div class="step-body">
            <div class="body-section">
              <div class="body-section-title">Суть этапа</div>
              <div class="desc-block">${s.desc}</div>
            </div>
            <div class="body-section">
              <div class="body-section-title">Поток данных</div>
              <div class="data-flow"><div class="df-row">${flowHtml}</div></div>
            </div>
          </div>
        </div>`;
    });

    container.innerHTML = html;
  }

  function toggleStep(n) {
    document.getElementById('step-' + n).classList.toggle('open');
  }

  function toggleCheck(e, n) {
    e.stopPropagation();
    const cb = e.currentTarget;
    const card = document.getElementById('step-' + n);
    cb.classList.toggle('done');
    cb.textContent = cb.classList.contains('done') ? '✓' : '';
    card.classList.toggle('checked', cb.classList.contains('done'));
    updateProgress();
  }

  // ═══════════════════════════════════════════
  // DB TABLES
  // ═══════════════════════════════════════════

  function renderDbTables() {
    const grid = document.getElementById('dbGrid');
    grid.innerHTML = DB_TABLES.map(t => `
      <div class="db-card ${t.status} group-${t.group}">
        <div class="db-name">${t.name}</div>
        <div class="db-desc">${t.desc}</div>
        <span class="db-badge">${t.status === 'done' ? '✓ реализована' : '○ новая'}</span>
      </div>
    `).join('');
  }

  // ═══════════════════════════════════════════
  // TECH STACK
  // ═══════════════════════════════════════════

  function renderTechStack() {
    document.getElementById('techGrid').innerHTML = TECH_STACK.map(t => `
      <div class="tool-card">
        <div class="tool-icon">${t.icon}</div>
        <div><div class="tool-name">${t.name}</div><div class="tool-use">${t.use}</div></div>
      </div>
    `).join('');
  }

  // ═══════════════════════════════════════════
  // RESULTS
  // ═══════════════════════════════════════════

  function renderResults() {
    document.getElementById('resultsGrid').innerHTML = RESULTS.map(r => `
      <div class="result-card">
        <div class="result-icon">${r.icon}</div>
        <div class="result-title">${r.title}</div>
        <div class="result-desc">${r.desc}</div>
      </div>
    `).join('');
  }

  // ═══════════════════════════════════════════
  // FORM HANDLING
  // ═══════════════════════════════════════════

  function switchForm(type, btn, formId) {
    const section = btn.closest('.contact-section');
    section.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    const phonePanel = document.getElementById('phonePanel-' + formId);
    const msgPanel = document.getElementById('messengerPanel-' + formId);

    if (type === 'phone') {
      phonePanel.classList.remove('hide');
      msgPanel.classList.remove('show');
    } else {
      phonePanel.classList.add('hide');
      msgPanel.classList.add('show');
    }
  }

  function selectMessenger(btn, formId) {
    const panel = document.getElementById('messengerPanel-' + formId);
    panel.querySelectorAll('.msg-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
  }

  function submitForm(which) {
    const successEl = document.getElementById('success-' + which);
    const wrap = successEl.closest('.contact-section');

    // Hide everything except success
    wrap.querySelectorAll('.form-fields, .form-toggle, .phone-panel, .messenger-panel, .form-submit, .form-note, h3, .sub, .extended-fields').forEach(el => {
      el.style.display = 'none';
    });
    successEl.style.display = 'block';
  }

  // ═══════════════════════════════════════════
  // AUTH
  // ═══════════════════════════════════════════

  function login() {
    const now = Date.now();

    if (now < lockUntil) return;

    const pwd = document.getElementById('loginPassword').value;
    const errorEl = document.getElementById('loginError');
    const lockEl = document.getElementById('loginLockout');

    if (pwd === AUTH_PASSWORD) {
      document.getElementById('loginBox').style.display = 'none';
      document.getElementById('authContent').classList.add('visible');
      errorEl.style.display = 'none';
      renderClients();
    } else {
      loginAttempts++;
      errorEl.style.display = 'block';

      if (loginAttempts >= 5) {
        lockUntil = now + 10 * 60 * 1000; // 10 min
        errorEl.style.display = 'none';
        lockEl.style.display = 'block';
        document.querySelector('#loginBox input').disabled = true;
        document.querySelector('#loginBox button').disabled = true;

        lockInterval = setInterval(() => {
          const remaining = Math.max(0, lockUntil - Date.now());
          if (remaining <= 0) {
            clearInterval(lockInterval);
            lockEl.style.display = 'none';
            document.querySelector('#loginBox input').disabled = false;
            document.querySelector('#loginBox button').disabled = false;
            loginAttempts = 0;
          } else {
            const m = Math.floor(remaining / 60000);
            const s = Math.floor((remaining % 60000) / 1000);
            document.getElementById('lockTimer').textContent = `${m}:${String(s).padStart(2,'0')}`;
          }
        }, 1000);
      }
    }
  }

  function logout() {
    document.getElementById('loginBox').style.display = 'block';
    document.getElementById('authContent').classList.remove('visible');
    document.getElementById('loginPassword').value = '';
    expandedClient = null;
  }

  // ═══════════════════════════════════════════
  // CLIENTS LIST + TRACKER
  // ═══════════════════════════════════════════

  function renderClients() {
    const list = document.getElementById('clientsList');
    let html = '';

    MOCK_CLIENTS.forEach(c => {
      const deltaClass = c.delta > 0 ? 'up' : (c.delta < 0 ? 'down' : 'flat');
      const deltaSymbol = c.delta > 0 ? '↑' : (c.delta < 0 ? '↓' : '→');
      const dynSymbol = c.stats.dynamicPct > 0 ? '↑' : (c.stats.dynamicPct < 0 ? '↓' : '→');
      const dynClass = c.stats.dynamicPct >= 0 ? 'green' : '';

      html += `
        <div class="client-row" id="client-${c.key}" onclick="Panel3.toggleClient('${c.key}')">
          <div class="priority-dot ${c.priority}"></div>
          <div>
            <div class="client-name">${c.name}</div>
            <div class="client-cat">${c.category}</div>
          </div>
          <div class="client-cpl">CPL ${c.cpl}₽</div>
          <div class="client-delta ${deltaClass}">${deltaSymbol}${Math.abs(c.delta)}%</div>
          <div class="client-expand-icon">▼</div>
        </div>
        <div class="tracker-wrap" id="tracker-${c.key}">
          <div class="tracker-inner">
            <div class="tracker-stats">
              <div class="tracker-stat">
                <div class="tracker-stat-val">${c.stats.ads.toLocaleString()}</div>
                <div class="tracker-stat-label">Объявлений</div>
              </div>
              <div class="tracker-stat">
                <div class="tracker-stat-val yellow">${c.stats.spend7d.toLocaleString()} ₽</div>
                <div class="tracker-stat-label">Расход / 7д</div>
              </div>
              <div class="tracker-stat">
                <div class="tracker-stat-val">${c.stats.contacts7d}</div>
                <div class="tracker-stat-label">Контактов / 7д</div>
              </div>
              <div class="tracker-stat">
                <div class="tracker-stat-val ${dynClass}">${dynSymbol}${Math.abs(c.stats.dynamicPct)}%</div>
                <div class="tracker-stat-label">Динамика</div>
              </div>
            </div>
            <div class="tracker-stages">
              ${c.stages.map((st, si) => `
                <div class="tracker-stage">
                  <div class="stage-check ${st.done ? 'done' : ''}" onclick="Panel3.toggleStage('${c.key}', ${si}); event.stopPropagation();">${st.done ? '✓' : ''}</div>
                  <div class="stage-body">
                    <div class="stage-title">${si+1}. ${STAGE_NAMES[si]}</div>
                    ${st.note ? `<div style="font-size: 12px; color: #8a8280; margin-top: 2px;">${st.note}</div>` : ''}
                    <div class="stage-note-toggle" onclick="Panel3.toggleNote('${c.key}', ${si}); event.stopPropagation();">✏️ Заметка</div>
                    <div class="stage-note-area" id="note-${c.key}-${si}">
                      <textarea placeholder="Заметка для отдела продаж...">${st.note || ''}</textarea>
                    </div>
                  </div>
                </div>
              `).join('')}
            </div>
          </div>
        </div>`;
    });

    list.innerHTML = html;
  }

  function toggleClient(key) {
    const row = document.getElementById('client-' + key);
    const tracker = document.getElementById('tracker-' + key);

    if (expandedClient === key) {
      row.classList.remove('expanded');
      tracker.classList.remove('visible');
      expandedClient = null;
    } else {
      // Collapse previous
      if (expandedClient) {
        document.getElementById('client-' + expandedClient).classList.remove('expanded');
        document.getElementById('tracker-' + expandedClient).classList.remove('visible');
      }
      row.classList.add('expanded');
      tracker.classList.add('visible');
      expandedClient = key;
    }
  }

  function toggleStage(clientKey, stageIndex) {
    const client = MOCK_CLIENTS.find(c => c.key === clientKey);
    if (!client) return;

    client.stages[stageIndex].done = !client.stages[stageIndex].done;

    // Update UI
    const stages = document.querySelectorAll(`#tracker-${clientKey} .stage-check`);
    const check = stages[stageIndex];
    check.classList.toggle('done');
    check.textContent = check.classList.contains('done') ? '✓' : '';
  }

  function toggleNote(clientKey, stageIndex) {
    const noteArea = document.getElementById(`note-${clientKey}-${stageIndex}`);
    noteArea.classList.toggle('visible');
    if (noteArea.classList.contains('visible')) {
      noteArea.querySelector('textarea').focus();
    }
  }

  // ═══════════════════════════════════════════
  // PUBLIC API
  // ═══════════════════════════════════════════

  return {
    switchForm,
    selectMessenger,
    submitForm,
    toggleStep,
    toggleCheck,
    login,
    logout,
    toggleClient,
    toggleStage,
    toggleNote,
    init
  };

})();

// ═══ INIT ON LOAD ═══
document.addEventListener('DOMContentLoaded', Panel3.init);
