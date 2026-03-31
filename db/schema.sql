-- ═══════════════════════════════════════════
-- schema.sql — Единая схема БД mnogunik
-- Все модули используют одну SQLite БД
-- ═══════════════════════════════════════════

-- ─── ГРУППА: stats (stavmnog пишет, панель 2 читает) ───

CREATE TABLE IF NOT EXISTS items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,           -- "svai_alx"
    avito_id        TEXT,                    -- ID на Авито
    internal_id     TEXT,                    -- наш ID (из generator)
    title           TEXT,
    category        TEXT,
    status          TEXT DEFAULT 'active',   -- active/blocked/recreated/closed
    parent_avito_id TEXT,                    -- если пересоздано после блокировки
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS item_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    avito_id        TEXT NOT NULL,
    date            TEXT NOT NULL,           -- YYYY-MM-DD
    views           INTEGER DEFAULT 0,       -- uniqViews
    contacts        INTEGER DEFAULT 0,       -- uniqContacts
    favorites       INTEGER DEFAULT 0,       -- uniqFavorites
    expenses        REAL DEFAULT 0,          -- расход в рублях
    UNIQUE(client_key, avito_id, date)
);

CREATE TABLE IF NOT EXISTS current_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    avito_id        TEXT NOT NULL,
    -- Текущие 7 дней
    views_7d        INTEGER DEFAULT 0,
    contacts_7d     INTEGER DEFAULT 0,
    favorites_7d    INTEGER DEFAULT 0,
    expenses_7d     REAL DEFAULT 0,
    ctr_7d          REAL DEFAULT 0,
    cvr_7d          REAL DEFAULT 0,
    cpl_7d          REAL DEFAULT 0,
    cpv_7d          REAL DEFAULT 0,
    -- Предыдущие 7 дней
    views_prev      INTEGER DEFAULT 0,
    contacts_prev   INTEGER DEFAULT 0,
    expenses_prev   REAL DEFAULT 0,
    ctr_prev        REAL DEFAULT 0,
    cpl_prev        REAL DEFAULT 0,
    -- Дельты
    views_delta     REAL DEFAULT 0,
    contacts_delta  REAL DEFAULT 0,
    ctr_delta       REAL DEFAULT 0,
    cpl_delta       REAL DEFAULT 0,
    -- Ставки
    bid_calculated  REAL DEFAULT 0,
    bid_code        TEXT DEFAULT '',
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(client_key, avito_id)
);

CREATE TABLE IF NOT EXISTS bids_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    avito_id        TEXT NOT NULL,
    bid_value       REAL NOT NULL,
    daily_limit     REAL,
    source          TEXT DEFAULT 'auto',     -- auto/manual
    applied_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS blocked_ads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    avito_id        TEXT NOT NULL,
    blocked_at      TEXT DEFAULT (datetime('now')),
    new_avito_id    TEXT,                    -- ID пересозданного
    recreated_at    TEXT
);

CREATE TABLE IF NOT EXISTS ad_links (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    old_avito_id    TEXT NOT NULL,
    new_avito_id    TEXT NOT NULL,
    linked_at       TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sync_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    operation       TEXT NOT NULL,           -- download/build/export/apply
    status          TEXT DEFAULT 'running',  -- running/done/error
    started_at      TEXT DEFAULT (datetime('now')),
    finished_at     TEXT,
    error           TEXT,
    rows_processed  INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS hypothesis_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    hypothesis_id   TEXT NOT NULL,
    period_start    TEXT NOT NULL,
    period_end      TEXT NOT NULL,
    views           INTEGER DEFAULT 0,
    contacts        INTEGER DEFAULT 0,
    expenses        REAL DEFAULT 0,
    ctr             REAL DEFAULT 0,
    cpl             REAL DEFAULT 0,
    updated_at      TEXT DEFAULT (datetime('now'))
);

-- ─── ГРУППА: gen (generator пишет) ───

CREATE TABLE IF NOT EXISTS generation_tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    status          TEXT DEFAULT 'pending',  -- pending/running/done/error/stopped
    params_json     TEXT,                    -- JSON с параметрами
    started_at      TEXT,
    finished_at     TEXT,
    ads_count       INTEGER DEFAULT 0,
    error           TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS accounts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    name            TEXT NOT NULL,
    avito_client_id TEXT,
    avito_user_id   TEXT,
    img_folder      TEXT,
    status          TEXT DEFAULT 'active',
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS city_groups (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    group_name      TEXT NOT NULL,
    cities_json     TEXT,                    -- JSON массив городов
    ad_limit        INTEGER DEFAULT 100
);

CREATE TABLE IF NOT EXISTS hypotheses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    group_name      TEXT,
    hypothesis_id   TEXT NOT NULL,           -- составной: 1-34-promo23-35-12-3
    description     TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS hypothesis_vars (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    hypothesis_id   INTEGER REFERENCES hypotheses(id),
    var_type        TEXT NOT NULL,           -- images/template_first/params/text/title
    var_id          TEXT NOT NULL,
    var_value       TEXT                     -- имя папки / имя файла / текст
);

CREATE TABLE IF NOT EXISTS templates_text (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    name            TEXT NOT NULL,
    file_path       TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS templates_img (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    name            TEXT NOT NULL,
    file_path       TEXT,
    config_json     TEXT,                    -- JSON параметров уникализации
    created_at      TEXT DEFAULT (datetime('now'))
);

-- ─── ГРУППА: crm (панель 3 пишет/читает) ───

CREATE TABLE IF NOT EXISTS leads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT,
    phone           TEXT,
    messenger       TEXT,                    -- WhatsApp/Telegram/VK
    messenger_contact TEXT,
    task_description TEXT,
    -- Расширенные поля (нижняя форма)
    pricelist_status TEXT,
    photos_status   TEXT,
    design_status   TEXT,
    categories_info TEXT,
    budget_info     TEXT,
    avito_account   TEXT,
    urgency         TEXT,
    source          TEXT DEFAULT 'landing',  -- landing/panel/manual
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS clients (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    category        TEXT,
    priority        TEXT DEFAULT 'ok',       -- bad/ok/good
    cpl_current     REAL,
    cpl_delta       REAL,
    status          TEXT DEFAULT 'active',
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS lead_stages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id       INTEGER REFERENCES clients(id),
    stage_number    INTEGER NOT NULL,        -- 1-8
    is_done         INTEGER DEFAULT 0,
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS lead_stage_notes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    stage_id        INTEGER REFERENCES lead_stages(id),
    note            TEXT,
    created_by      TEXT DEFAULT 'specialist',
    created_at      TEXT DEFAULT (datetime('now'))
);

-- ─── ГРУППА: config (общее) ───

CREATE TABLE IF NOT EXISTS bid_formulas (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    group_name      TEXT,
    formula_name    TEXT DEFAULT 'default',
    min_bid         REAL DEFAULT 0,
    max_bid         REAL DEFAULT 0,
    coefficient     REAL DEFAULT 1.0,
    daily_limit     REAL DEFAULT 500,
    config_json     TEXT
);

CREATE TABLE IF NOT EXISTS account_limits (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    client_key      TEXT NOT NULL,
    category        TEXT,
    city_group      TEXT,
    max_ads         INTEGER DEFAULT 100,
    current_ads     INTEGER DEFAULT 0
);

-- ─── ИНДЕКСЫ ───

CREATE INDEX IF NOT EXISTS idx_item_stats_client_date ON item_stats(client_key, date);
CREATE INDEX IF NOT EXISTS idx_item_stats_avito ON item_stats(avito_id);
CREATE INDEX IF NOT EXISTS idx_current_stats_client ON current_stats(client_key);
CREATE INDEX IF NOT EXISTS idx_bids_history_avito ON bids_history(avito_id);
CREATE INDEX IF NOT EXISTS idx_leads_created ON leads(created_at);
CREATE INDEX IF NOT EXISTS idx_clients_key ON clients(client_key);
CREATE INDEX IF NOT EXISTS idx_sync_log_client ON sync_log(client_key, operation);
