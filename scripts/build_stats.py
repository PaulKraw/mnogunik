"""
scripts/build_stats.py — Расчёт сводной аналитики (7д + пред.7д).

Использование:
    python -m stavmnog.scripts.build_stats --client=evg
"""

import argparse
import json
import os
import sqlite3
from datetime import datetime, timedelta

from shared.config import DB_PATH
from shared.logger import get_logger

from stavmnog.config import STATUS_DIR, LOG_DIR
from stavmnog.utils.pid_lock import acquire_lock, release_lock
from stavmnog.utils.formulas import calc_bid, safe_div, delta_pct


def _write_status(client_key, data):
    os.makedirs(STATUS_DIR, exist_ok=True)
    path = os.path.join(STATUS_DIR, f"build_stats_{client_key}.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def run(client_key: str):
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = get_logger(
        f"build_{client_key}",
        os.path.join(LOG_DIR, f"build_stats_{client_key}.log"),
    )

    if not acquire_lock("build_stats", client_key, logger):
        return

    logger.info("=" * 50)
    logger.info(f"СТАРТ | build_stats | {client_key}")
    logger.info("=" * 50)

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_status(client_key, {
        "operation": "build_stats", "client": client_key,
        "status": "running", "started_at": started_at,
        "finished_at": None, "items_processed": 0, "error": None,
    })

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        today = datetime.now().date()
        period_from = today - timedelta(days=6)
        period_to = today
        prev_from = today - timedelta(days=13)
        prev_to = today - timedelta(days=7)

        logger.info(f"Период 7д: {period_from} → {period_to}")
        logger.info(f"Пред. 7д:  {prev_from} → {prev_to}")

        # Агрегация одним запросом
        cur = conn.execute("""
            SELECT
                item_id,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN impressions ELSE 0 END) AS imp_7d,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN views        ELSE 0 END) AS views_7d,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN contacts     ELSE 0 END) AS contacts_7d,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN favorites    ELSE 0 END) AS fav_7d,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN all_spend    ELSE 0 END) AS spend_7d,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN impressions ELSE 0 END) AS imp_prev,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN views        ELSE 0 END) AS views_prev,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN contacts     ELSE 0 END) AS contacts_prev,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN favorites    ELSE 0 END) AS fav_prev,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN all_spend    ELSE 0 END) AS spend_prev
            FROM item_stats
            WHERE client_key = ? AND stat_date BETWEEN ? AND ?
            GROUP BY item_id
        """, (
            str(period_from), str(period_to),
            str(period_from), str(period_to),
            str(period_from), str(period_to),
            str(period_from), str(period_to),
            str(period_from), str(period_to),
            str(prev_from), str(prev_to),
            str(prev_from), str(prev_to),
            str(prev_from), str(prev_to),
            str(prev_from), str(prev_to),
            str(prev_from), str(prev_to),
            client_key, str(prev_from), str(period_to),
        ))
        rows = cur.fetchall()
        logger.info(f"Строк после агрегации: {len(rows)}")

        if not rows:
            logger.warning("Нет данных — запусти download.py")
            return

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        upsert_rows = []

        for r in rows:
            imp_7d = r["imp_7d"] or 0
            views_7d = r["views_7d"] or 0
            contacts_7d = r["contacts_7d"] or 0
            fav_7d = r["fav_7d"] or 0
            spend_7d = r["spend_7d"] or 0.0

            views_prev = r["views_prev"] or 0
            contacts_prev = r["contacts_prev"] or 0
            spend_prev = r["spend_prev"] or 0.0

            ctr_7d = safe_div(views_7d, imp_7d)
            cvr_7d = safe_div(contacts_7d, views_7d)
            cpl_7d = safe_div(spend_7d, contacts_7d)
            cpv_7d = safe_div(spend_7d, views_7d)

            ctr_prev = safe_div(views_prev, r["imp_prev"] or 0)
            cvr_prev = safe_div(contacts_prev, views_prev)
            cpl_prev = safe_div(spend_prev, contacts_prev)
            cpv_prev = safe_div(spend_prev, views_prev)

            bid_code = calc_bid(0, 0, 0, ctr_7d, views_7d, contacts_7d)

            upsert_rows.append((
                r["item_id"], client_key,
                str(period_from), str(period_to),
                imp_7d, views_7d, contacts_7d, fav_7d, round(spend_7d, 2),
                ctr_7d, cvr_7d, cpl_7d, cpv_7d,
                r["imp_prev"] or 0, views_prev, contacts_prev,
                r["fav_prev"] or 0, round(spend_prev, 2),
                ctr_prev, cvr_prev, cpl_prev, cpv_prev,
                delta_pct(contacts_7d, contacts_prev),
                delta_pct(cpl_7d, cpl_prev),
                bid_code, None, now,
            ))

        conn.executemany("""
            INSERT OR REPLACE INTO current_stats (
                item_id, client_key, period_from, period_to,
                impressions_7d, views_7d, contacts_7d, favorites_7d, spend_7d,
                ctr_7d, cvr_7d, cpl_7d, cpv_7d,
                impressions_prev, views_prev, contacts_prev, favorites_prev, spend_prev,
                ctr_prev, cvr_prev, cpl_prev, cpv_prev,
                delta_contacts, delta_cpl,
                bid_code, limit_code, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, upsert_rows)
        conn.commit()

        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""
            INSERT INTO sync_log (client_key, operation, started_at, finished_at, status, result_json)
            VALUES (?, 'build_stats', ?, ?, 'done', ?)
        """, (client_key, started_at, finished_at,
              json.dumps({"items_processed": len(upsert_rows)})))
        conn.commit()

        _write_status(client_key, {
            "operation": "build_stats", "client": client_key,
            "status": "done", "started_at": started_at,
            "finished_at": finished_at,
            "items_processed": len(upsert_rows),
            "error": None,
        })

        logger.info(f"ГОТОВО | build_stats | {client_key} | {len(upsert_rows)} объявлений")

    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        _write_status(client_key, {
            "operation": "build_stats", "client": client_key,
            "status": "error", "started_at": started_at,
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "items_processed": 0, "error": str(e),
        })

    finally:
        conn.close()
        release_lock("build_stats", client_key)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", required=True)
    args = ap.parse_args()
    run(args.client)
