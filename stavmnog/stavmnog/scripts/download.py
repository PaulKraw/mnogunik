"""
download.py — скачивание статистики объявлений с Авито в SQLite

Использование:
    python scripts/download.py --client=evg
    python scripts/download.py --client=evg --days=30
"""

import argparse
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta

import requests
import signal
import atexit

# ---------------------------------------------------------------------------
# Пути
# ---------------------------------------------------------------------------
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "clients.json")
DB_PATH     = os.path.join(BASE_DIR, "data", "avito.db")
STATUS_DIR  = os.path.join(BASE_DIR, "status")
LOG_DIR     = os.path.join(BASE_DIR, "logs")

# ---------------------------------------------------------------------------
# Настройки периода
#
# REWRITE_LAST_DAYS — сколько последних записанных дней перезаписывать.
#   1 = только последний день (он мог быть неполным — записан в середине дня)
#   3 = последний день + ещё 2 дня назад
#
# Пример: последняя дата в БД = 10 марта, REWRITE_LAST_DAYS = 3
#   → скачиваем с 8 марта по сегодня (8, 9, 10 перезаписываем, 11+ новые)
# ---------------------------------------------------------------------------
REWRITE_LAST_DAYS = 1

# ---------------------------------------------------------------------------
# Авито API
# ---------------------------------------------------------------------------
AVITO_TOKEN_URL = "https://api.avito.ru/token"
AVITO_STATS_URL = "https://api.avito.ru/stats/v2/accounts/{user_id}/items"
RATE_LIMIT_SEC  = 65

METRICS = [
    "impressions", "views", "contacts", "favorites",
    "presenceSpending", "promoSpending", "allSpending",
    "averageViewCost", "averageContactCost",
]
MONEY_SLUGS = {
    "presenceSpending", "promoSpending", "allSpending",
    "averageViewCost",  "averageContactCost",
}
SLUG_TO_COL = {
    "impressions":        "impressions",
    "views":              "views",
    "contacts":           "contacts",
    "favorites":          "favorites",
    "presenceSpending":   "presence_spend",
    "promoSpending":      "promo_spend",
    "allSpending":        "all_spend",
    "averageViewCost":    "avg_view_cost",
    "averageContactCost": "avg_contact_cost",
}


# ---------------------------------------------------------------------------
# Логирование
# Имя логгера включает PID — гарантирует уникальность, нет дублей в логах
# ---------------------------------------------------------------------------
def setup_logger(client_key: str) -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"download_{client_key}.log")
    name     = f"dl_{client_key}_{os.getpid()}"
    logger   = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        fh  = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Стоп-флаг
# ---------------------------------------------------------------------------
def stop_requested(client_key: str) -> bool:
    return os.path.exists(
        os.path.join(STATUS_DIR, f"stop_download_{client_key}.flag")
    )

def clear_stop_flag(client_key: str):
    flag = os.path.join(STATUS_DIR, f"stop_download_{client_key}.flag")
    if os.path.exists(flag):
        os.remove(flag)


# ---------------------------------------------------------------------------
# Защита от двойного запуска через PID-файл (без fcntl, кросс-платформенно)
# ---------------------------------------------------------------------------
def acquire_lock(client_key: str, logger) -> bool:
    os.makedirs(STATUS_DIR, exist_ok=True)
    pid_path = os.path.join(STATUS_DIR, f"download_{client_key}.pid")

    if os.path.exists(pid_path):
        try:
            old_pid = int(open(pid_path).read().strip())
            try:
                os.kill(old_pid, 0)
                logger.warning(
                    f"download {client_key} уже запущен (pid={old_pid}) — выходим"
                )
                return False
            except (OSError, ProcessLookupError):
                pass  # процесс мёртв — можно продолжать
        except (ValueError, IOError):
            pass

    with open(pid_path, "w") as f:
        f.write(str(os.getpid()))

    # гарантируем удаление PID-файла при любом завершении
    def _cleanup():
        try:
            if os.path.exists(pid_path):
                os.remove(pid_path)
        except Exception:
            pass

    atexit.register(_cleanup)

    def _sig_handler(sig, frame):
        _cleanup()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _sig_handler)

    return True

def release_lock(client_key: str):
    pid_path = os.path.join(STATUS_DIR, f"download_{client_key}.pid")
    try:
        if os.path.exists(pid_path):
            os.remove(pid_path)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Status JSON
# ---------------------------------------------------------------------------
def write_status(client_key: str, data: dict):
    os.makedirs(STATUS_DIR, exist_ok=True)
    path = os.path.join(STATUS_DIR, f"download_{client_key}.json")
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Авито: токен
# ---------------------------------------------------------------------------
def get_token(client_id: str, client_secret: str) -> str:
    r = requests.post(
        AVITO_TOKEN_URL,
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        },
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]


# ---------------------------------------------------------------------------
# Авито: статистика за один день
# ---------------------------------------------------------------------------
def fetch_stats_for_date(token: str, user_id: str, date_str: str, logger) -> list:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
    }
    all_items = []
    offset    = 0
    total     = None

    while True:
        payload = {
            "dateFrom": date_str, "dateTo": date_str,
            "grouping": "item",   "limit":  1000,
            "offset":   offset,   "metrics": METRICS,
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
        resp_json = r.json()

        if "result" not in resp_json:
            logger.info(f"  Нет данных за {date_str} (пустой ответ)")
            return []

        data = resp_json["result"]

        if "dataTotalCount" not in data:
            logger.info(f"  Нет активности за {date_str}")
            return []

        if total is None:
            total = data["dataTotalCount"]
            logger.info(f"  Объявлений с активностью: {total}")

        for g in data.get("groupings") or []:
            row = {"item_id": g["id"]}
            for m in g["metrics"]:
                slug = m["slug"]
                val  = m["value"]
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


# ---------------------------------------------------------------------------
# SQLite: определяем период скачивания
# ---------------------------------------------------------------------------
def get_period(conn: sqlite3.Connection, client_key: str, force_days, logger, rewrite_days=None) -> tuple:
    today = datetime.now().date()

    if force_days:
        date_from = today - timedelta(days=force_days - 1)
        logger.info(f"Принудительный режим: последние {force_days} дней")
        return str(date_from), str(today)

    cur = conn.execute(
        "SELECT MAX(stat_date) FROM item_stats WHERE client_key = ?",
        (client_key,),
    )
    last_date = cur.fetchone()[0]

    if last_date is None:
        date_from = today - timedelta(days=29)
        logger.info("Первый запуск — качаем последние 30 дней")
    else:
        last      = datetime.strptime(last_date, "%Y-%m-%d").date()
        # rewrite_days из аргумента или из константы
        rw = rewrite_days if rewrite_days is not None else REWRITE_LAST_DAYS
        rw = max(1, int(rw))
        date_from = last - timedelta(days=rw - 1)
        logger.info(f"Последняя дата в БД: {last_date}")
        logger.info(
            f"Перезаписываем последние {rw} дн "
            f"(с {date_from}), докачиваем новые до {today}"
        )

    return str(date_from), str(today)


# ---------------------------------------------------------------------------
# SQLite: сохраняем один день
# ---------------------------------------------------------------------------
def save_day(conn: sqlite3.Connection, client_key: str, date_str: str, items: list):
    now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        (
            item["item_id"], client_key, date_str,
            item.get("impressions", 0),    item.get("views", 0),
            item.get("contacts", 0),       item.get("favorites", 0),
            item.get("presence_spend", 0), item.get("promo_spend", 0),
            item.get("all_spend", 0),      item.get("avg_view_cost", 0),
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


# ---------------------------------------------------------------------------
# SQLite: sync_log
# ---------------------------------------------------------------------------
def log_sync(conn, client_key, started_at, finished_at, status, result: dict):
    conn.execute("""
        INSERT INTO sync_log
            (client_key, operation, started_at, finished_at, status, result_json)
        VALUES (?, 'download', ?, ?, ?, ?)
    """, (client_key, started_at, finished_at, status,
          json.dumps(result, ensure_ascii=False)))
    conn.commit()


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------
def run(client_key: str, force_days=None, rewrite_days=None):
    logger = setup_logger(client_key)

    if not acquire_lock(client_key, logger):
        return

    logger.info("=" * 50)
    logger.info(f"СТАРТ | download | {client_key} | pid={os.getpid()}")
    logger.info("=" * 50)

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        clients = json.load(f)

    if client_key not in clients:
        logger.error(f"Клиент '{client_key}' не найден в clients.json")
        release_lock(client_key)
        return

    cfg     = clients[client_key]
    user_id = cfg["user_id"]

    write_status(client_key, {
        "operation": "download", "client": client_key,
        "status": "running",    "started_at": started_at,
        "finished_at": None,    "period_from": None,
        "period_to": None,      "rows_fetched": 0,
        "total_spend_rub": 0,   "error": None,
    })

    conn = sqlite3.connect(DB_PATH)

    try:
        logger.info("Запрашиваем токен Авито...")
        token = get_token(cfg["client_id"], cfg["client_secret"])
        logger.info("Токен получен OK")

        date_from, date_to = get_period(conn, client_key, force_days, logger, rewrite_days)
        logger.info(f"Итоговый период: {date_from} -> {date_to}")

        write_status(client_key, {
            "operation": "download", "client": client_key,
            "status": "running",    "started_at": started_at,
            "finished_at": None,    "period_from": date_from,
            "period_to": date_to,   "rows_fetched": 0,
            "total_spend_rub": 0,   "error": None,
        })

        total_rows  = 0
        total_spend = 0.0
        current     = datetime.strptime(date_from, "%Y-%m-%d").date()
        end         = datetime.strptime(date_to,   "%Y-%m-%d").date()
        days_total  = (end - current).days + 1

        clear_stop_flag(client_key)

        day_num = 0
        while current <= end:
            if stop_requested(client_key):
                logger.info("СТОП — сигнал из панели")
                break

            day_num += 1
            date_str = str(current)
            logger.info(f"[{day_num}/{days_total}] {date_str}")

            items     = fetch_stats_for_date(token, user_id, date_str, logger)
            save_day(conn, client_key, date_str, items)

            day_spend    = sum(i.get("all_spend", 0) for i in items)
            total_rows  += len(items)
            total_spend += day_spend

            logger.info(
                f"  -> {len(items)} объявл | {day_spend:.2f} руб"
                + (f" | всего: {total_rows} / {total_spend:.2f} руб" if len(items) > 0 else "")
            )

            current += timedelta(days=1)

            if current <= end:
                if len(items) > 0:
                    logger.info(f"  -> пауза {RATE_LIMIT_SEC} сек...")
                    time.sleep(RATE_LIMIT_SEC)
                else:
                    time.sleep(2)

        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = {
            "period_from":     date_from,
            "period_to":       date_to,
            "rows_fetched":    total_rows,
            "total_spend_rub": round(total_spend, 2),
        }

        write_status(client_key, {
            "operation": "download", "client": client_key,
            "status": "done",       "started_at": started_at,
            "finished_at": finished_at, **result, "error": None,
        })
        log_sync(conn, client_key, started_at, finished_at, "done", result)

        logger.info("=" * 50)
        logger.info(f"ГОТОВО | {client_key} | строк: {total_rows} | {total_spend:.2f} руб")
        logger.info("=" * 50)

    except Exception as e:
        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.error(f"Ошибка: {e}", exc_info=True)
        write_status(client_key, {
            "operation": "download", "client": client_key,
            "status": "error",      "started_at": started_at,
            "finished_at": finished_at, "period_from": None,
            "period_to": None,      "rows_fetched": 0,
            "total_spend_rub": 0,   "error": str(e),
        })
        log_sync(conn, client_key, started_at, finished_at,
                 "error", {"error": str(e)})

    finally:
        conn.close()
        release_lock(client_key)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Скачать статистику Авито в SQLite")
    ap.add_argument("--client", required=True, help="Ключ клиента из clients.json")
    ap.add_argument("--days",   type=int, default=None,
                    help="Принудительно за N дней (игнорирует логику БД)")
    ap.add_argument("--rewrite", type=int, default=None,
                    help="Перезаписывать последних N дней (переопределяет REWRITE_LAST_DAYS)")
    args = ap.parse_args()
    run(args.client, args.days, rewrite_days=args.rewrite)