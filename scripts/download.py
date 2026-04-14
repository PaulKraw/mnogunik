"""
scripts/download.py — Скачивание статистики объявлений Авито → SQLite.

Использование:
    python -m stavmnog.scripts.download --client=evg
    python -m stavmnog.scripts.download --client=evg --days=30 --rewrite=3
"""

import argparse
import json
import os
import sqlite3
import time
from datetime import datetime, timedelta

import requests

from shared.config import DB_PATH
from shared.avito_api import get_avito_token
from shared.logger import write_log, write_status, get_logger

from stavmnog.config import (
    get_client_config, STATUS_DIR, LOG_DIR,
    AVITO_STATS_URL, RATE_LIMIT_SEC, METRICS, MONEY_SLUGS, SLUG_TO_COL,
)
from stavmnog.utils.pid_lock import acquire_lock, release_lock

REWRITE_LAST_DAYS = 1


# ═══════════════════════════════════════════
# СТОП-ФЛАГ
# ═══════════════════════════════════════════

def stop_requested(client_key: str) -> bool:
    return os.path.exists(os.path.join(STATUS_DIR, f"stop_download_{client_key}.flag"))


def clear_stop_flag(client_key: str) -> None:
    flag = os.path.join(STATUS_DIR, f"stop_download_{client_key}.flag")
    if os.path.exists(flag):
        os.remove(flag)


# ═══════════════════════════════════════════
# СТАТИСТИКА ЗА ДЕНЬ
# ═══════════════════════════════════════════

def fetch_stats_for_date(token: str, user_id: str, date_str: str, logger) -> list:
    """Запрашивает статистику за один день через Avito API."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    all_items = []
    offset = 0
    total = None

    while True:
        payload = {
            "dateFrom": date_str, "dateTo": date_str,
            "grouping": "item", "limit": 1000,
            "offset": offset, "metrics": METRICS,
        }
        r = requests.post(
            AVITO_STATS_URL.format(user_id=user_id),
            headers=headers, json=payload, timeout=30,
        )
        if r.status_code == 429:
            logger.warning(f"429 rate limit — ждём {RATE_LIMIT_SEC} сек")
            time.sleep(RATE_LIMIT_SEC)
            continue

        r.raise_for_status()
        resp = r.json()

        if "result" not in resp:
            return []

        data = resp["result"]
        if "dataTotalCount" not in data:
            return []

        if total is None:
            total = data["dataTotalCount"]
            logger.info(f"  Объявлений с активностью: {total}")

        for g in data.get("groupings") or []:
            row = {"item_id": g["id"]}
            for m in g["metrics"]:
                slug = m["slug"]
                val = m["value"]
                if slug in MONEY_SLUGS:
                    val = round(val / 100, 2)
                col = SLUG_TO_COL.get(slug)
                if col:
                    row[col] = val
            all_items.append(row)

        offset += 1000
        if total == 0 or offset >= total:
            break

        logger.info(f"  Пагинация offset={offset}, ждём {RATE_LIMIT_SEC} сек...")
        time.sleep(RATE_LIMIT_SEC)

    return all_items


# ═══════════════════════════════════════════
# SQLITE
# ═══════════════════════════════════════════

def get_period(conn, client_key, force_days, logger, rewrite_days=None):
    today = datetime.now().date()

    if force_days:
        date_from = today - timedelta(days=force_days - 1)
        logger.info(f"Принудительный режим: последние {force_days} дней")
        return str(date_from), str(today)

    cur = conn.execute(
        "SELECT MAX(stat_date) FROM item_stats WHERE client_key = ?", (client_key,)
    )
    last_date = cur.fetchone()[0]

    if last_date is None:
        date_from = today - timedelta(days=29)
        logger.info("Первый запуск — качаем последние 30 дней")
    else:
        last = datetime.strptime(last_date, "%Y-%m-%d").date()
        rw = max(1, int(rewrite_days or REWRITE_LAST_DAYS))
        date_from = last - timedelta(days=rw - 1)
        logger.info(f"Последняя дата в БД: {last_date}, перезапись {rw} дн с {date_from}")

    return str(date_from), str(today)


def save_day(conn, client_key, date_str, items):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        (
            item["item_id"], client_key, date_str,
            item.get("impressions", 0), item.get("views", 0),
            item.get("contacts", 0), item.get("favorites", 0),
            item.get("presence_spend", 0), item.get("promo_spend", 0),
            item.get("all_spend", 0), item.get("avg_view_cost", 0),
            item.get("avg_contact_cost", 0), now,
        )
        for item in items
    ]
    if rows:
        conn.executemany("""
            INSERT OR REPLACE INTO item_stats
                (item_id, client_key, stat_date,
                 impressions, views, contacts, favorites,
                 presence_spend, promo_spend, all_spend,
                 avg_view_cost, avg_contact_cost, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════

def run(client_key: str, force_days=None, rewrite_days=None):
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = get_logger(
        f"dl_{client_key}",
        os.path.join(LOG_DIR, f"download_{client_key}.log"),
    )

    if not acquire_lock("download", client_key, logger):
        return

    logger.info("=" * 50)
    logger.info(f"СТАРТ | download | {client_key} | pid={os.getpid()}")
    logger.info("=" * 50)

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_file = os.path.join(STATUS_DIR, f"download_{client_key}.json")

    cfg = get_client_config(client_key)
    user_id = cfg["user_id"]

    write_status("running", f"Скачивание {client_key}...", "download", status_file)

    conn = sqlite3.connect(DB_PATH)

    try:
        token = get_avito_token(cfg["client_id"], cfg["client_secret"])
        logger.info("Токен Авито получен")

        date_from, date_to = get_period(conn, client_key, force_days, logger, rewrite_days)

        total_rows = 0
        total_spend = 0.0
        current = datetime.strptime(date_from, "%Y-%m-%d").date()
        end = datetime.strptime(date_to, "%Y-%m-%d").date()
        days_total = (end - current).days + 1

        clear_stop_flag(client_key)
        day_num = 0

        while current <= end:
            if stop_requested(client_key):
                logger.info("СТОП — сигнал из панели")
                break

            day_num += 1
            date_str = str(current)
            logger.info(f"[{day_num}/{days_total}] {date_str}")

            items = fetch_stats_for_date(token, user_id, date_str, logger)
            save_day(conn, client_key, date_str, items)

            day_spend = sum(i.get("all_spend", 0) for i in items)
            total_rows += len(items)
            total_spend += day_spend

            logger.info(f"  -> {len(items)} объявл | {day_spend:.2f} руб")

            current += timedelta(days=1)
            if current <= end:
                time.sleep(RATE_LIMIT_SEC if items else 2)

        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # sync_log
        conn.execute("""
            INSERT INTO sync_log (client_key, operation, started_at, finished_at, status, result_json)
            VALUES (?, 'download', ?, ?, 'done', ?)
        """, (client_key, started_at, finished_at,
              json.dumps({"rows_fetched": total_rows, "total_spend_rub": round(total_spend, 2)})))
        conn.commit()

        # status JSON для фронта
        _write_download_status(status_file, "done", started_at, finished_at,
                               date_from, date_to, total_rows, total_spend)

        logger.info(f"ГОТОВО | {client_key} | строк: {total_rows} | {total_spend:.2f} руб")

    except Exception as e:
        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.error(f"Ошибка: {e}", exc_info=True)
        _write_download_status(status_file, "error", started_at, finished_at, error=str(e))

    finally:
        conn.close()
        release_lock("download", client_key)


def _write_download_status(path, status, started_at, finished_at,
                            period_from=None, period_to=None,
                            rows_fetched=0, total_spend=0, error=None):
    """Записывает JSON-статус для фронтенда."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = {
        "operation": "download", "status": status,
        "started_at": started_at, "finished_at": finished_at,
        "period_from": period_from, "period_to": period_to,
        "rows_fetched": rows_fetched,
        "total_spend_rub": round(total_spend, 2) if total_spend else 0,
        "error": error,
    }
    import json as _json
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        _json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", required=True)
    ap.add_argument("--days", type=int, default=None)
    ap.add_argument("--rewrite", type=int, default=None)
    args = ap.parse_args()
    run(args.client, args.days, rewrite_days=args.rewrite)
