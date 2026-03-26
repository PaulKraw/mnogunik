import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "avito.db")


def create_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ---------------------------------------------------------------
    # 1. items — справочник объявлений
    # ---------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS items (
        item_id     INTEGER NOT NULL,
        client_key  TEXT    NOT NULL,
        title       TEXT,
        status      TEXT,
        price       REAL,
        url         TEXT,
        category    TEXT,
        updated_at  TEXT    DEFAULT (datetime('now')),
        PRIMARY KEY (item_id, client_key)
    )
    """)

    # ---------------------------------------------------------------
    # 2. item_stats — статистика по дням
    # ---------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS item_stats (
        item_id          INTEGER NOT NULL,
        client_key       TEXT    NOT NULL,
        stat_date        TEXT    NOT NULL,
        impressions      INTEGER DEFAULT 0,
        views            INTEGER DEFAULT 0,
        contacts         INTEGER DEFAULT 0,
        favorites        INTEGER DEFAULT 0,
        presence_spend   REAL    DEFAULT 0,
        promo_spend      REAL    DEFAULT 0,
        all_spend        REAL    DEFAULT 0,
        avg_view_cost    REAL    DEFAULT 0,
        avg_contact_cost REAL    DEFAULT 0,
        bid_rub          REAL    DEFAULT NULL,
        limit_rub        REAL    DEFAULT NULL,
        updated_at       TEXT    DEFAULT (datetime('now')),
        PRIMARY KEY (item_id, client_key, stat_date)
    )
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_client_date
        ON item_stats(client_key, stat_date)
    """)

    # ---------------------------------------------------------------
    # 3. current_stats — сводная аналитика (7д и пред.7д)
    # ---------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS current_stats (
        item_id           INTEGER NOT NULL,
        client_key        TEXT    NOT NULL,
        period_from       TEXT,
        period_to         TEXT,
        impressions_7d    INTEGER DEFAULT 0,
        views_7d          INTEGER DEFAULT 0,
        contacts_7d       INTEGER DEFAULT 0,
        favorites_7d      INTEGER DEFAULT 0,
        spend_7d          REAL    DEFAULT 0,
        ctr_7d            REAL    DEFAULT 0,
        cvr_7d            REAL    DEFAULT 0,
        cpl_7d            REAL    DEFAULT 0,
        cpv_7d            REAL    DEFAULT 0,
        impressions_prev  INTEGER DEFAULT 0,
        views_prev        INTEGER DEFAULT 0,
        contacts_prev     INTEGER DEFAULT 0,
        favorites_prev    INTEGER DEFAULT 0,
        spend_prev        REAL    DEFAULT 0,
        ctr_prev          REAL    DEFAULT 0,
        cvr_prev          REAL    DEFAULT 0,
        cpl_prev          REAL    DEFAULT 0,
        cpv_prev          REAL    DEFAULT 0,
        delta_contacts    REAL    DEFAULT 0,
        delta_cpl         REAL    DEFAULT 0,
        bid_code          REAL    DEFAULT NULL,
        limit_code        REAL    DEFAULT NULL,
        updated_at        TEXT    DEFAULT (datetime('now')),
        PRIMARY KEY (item_id, client_key)
    )
    """)

    # ---------------------------------------------------------------
    # 4. bids_history — история применённых ставок
    # ---------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS bids_history (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id       INTEGER NOT NULL,
        client_key    TEXT    NOT NULL,
        applied_at    TEXT    NOT NULL,
        bid_rub       REAL,
        final_bid_rub REAL,
        limit_rub     REAL,
        status        TEXT,
        message       TEXT,
        source        TEXT    DEFAULT 'sheet'
    )
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_bids_hist
        ON bids_history(client_key, applied_at)
    """)

    # ---------------------------------------------------------------
    # 5. sync_log — лог запусков из панели
    # ---------------------------------------------------------------
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sync_log (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        client_key   TEXT    NOT NULL,
        operation    TEXT    NOT NULL,
        started_at   TEXT,
        finished_at  TEXT,
        status       TEXT,
        result_json  TEXT
    )
    """)

    conn.commit()
    conn.close()

    print(f"✅ База создана: {DB_PATH}")
    print("Таблицы:")
    print("  - items          (справочник объявлений)")
    print("  - item_stats     (статистика по дням)")
    print("  - current_stats  (сводная аналитика 7д + пред.7д)")
    print("  - bids_history   (история применённых ставок)")
    print("  - sync_log       (лог запусков из панели)")

if __name__ == "__main__":
    create_db()