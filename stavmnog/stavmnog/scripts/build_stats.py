"""
build_stats.py — расчёт сводной аналитики (7д + пред.7д) и ставки из кода

Использование:
    python scripts/build_stats.py --client=evg
"""

import argparse
import json
import logging
import math
import os
import sqlite3
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Пути
# ---------------------------------------------------------------------------
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "clients.json")
DB_PATH     = os.path.join(BASE_DIR, "data", "avito.db")
STATUS_DIR  = os.path.join(BASE_DIR, "status")
LOG_DIR     = os.path.join(BASE_DIR, "logs")


# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------
def setup_logger(client_key: str) -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"build_stats_{client_key}.log")
    logger = logging.getLogger(f"build.stats.{client_key}")
    logger.setLevel(logging.INFO)
    # очищаем handlers чтобы не дублировать при повторном вызове
    logger.handlers.clear()
    logger.propagate = False
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Status JSON
# ---------------------------------------------------------------------------
def write_status(client_key: str, data: dict):
    os.makedirs(STATUS_DIR, exist_ok=True)
    path = os.path.join(STATUS_DIR, f"build_stats_{client_key}.json")
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Формула ставки (аналог Excel-формулы из листа Ставки)
#
# =МИН(макс;
#    ЕСЛИ(ctr=0;
#       ЕСЛИ(клики>10; ОКРУГЛВВЕРХ(мин/2); мин);
#       ЕСЛИ(ctr<0.03; 5;
#          ЕСЛИ(лиды=0; мин;
#             ЕСЛИ(лиды<=1;
#                МАКС(мин; ОКРУГЛ(пр_став*0.7; 0));
#                пр_став
#             )
#          )
#       )
#    )
# )
#
# Параметры:
#   minn     — мин ставка (колонка K)
#   maxx     — макс ставка (колонка L)
#   prev_bid — предыдущая ставка / корект (колонка H)
#   ctr      — конверсия показ→просмотр за 7д
#   clicks   — просмотры за 7д (колонка P)
#   leads    — контакты за 7д (колонка Q)
# ---------------------------------------------------------------------------
def calc_bid(minn: float, maxx: float, prev_bid: float,
             ctr: float, clicks: int, leads: int) -> float:

    # защита от None
    minn     = minn     or 0
    maxx     = maxx     or 0
    prev_bid = prev_bid or 0

    if ctr == 0:
        inner = math.ceil(minn / 2) if clicks > 10 else minn
    elif ctr < 0.03:
        inner = 5.0
    elif leads == 0:
        inner = minn
    elif leads <= 1:
        inner = max(minn, round(prev_bid * 0.7))
    else:
        inner = prev_bid

    result = min(maxx, inner) if maxx > 0 else inner
    return round(result, 2)


# ---------------------------------------------------------------------------
# Безопасное деление — возвращает 0 если делитель = 0
# ---------------------------------------------------------------------------
def safe_div(a, b) -> float:
    return round(a / b, 4) if b and b != 0 else 0.0


# ---------------------------------------------------------------------------
# Расчёт дельты в процентах
# ---------------------------------------------------------------------------
def delta_pct(new_val, old_val) -> float:
    if old_val and old_val != 0:
        return round((new_val - old_val) / old_val * 100, 2)
    return 0.0


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------
def run(client_key: str):
    logger = setup_logger(client_key)

    # защита от двойного запуска через PID-файл
    os.makedirs(STATUS_DIR, exist_ok=True)
    pid_path = os.path.join(STATUS_DIR, f"build_stats_{client_key}.pid")
    if os.path.exists(pid_path):
        try:
            old_pid = int(open(pid_path).read().strip())
            try:
                os.kill(old_pid, 0)
                logger.warning(f"build_stats {client_key} уже запущен (pid={old_pid}) — выходим")
                return
            except (OSError, ProcessLookupError):
                pass
        except (ValueError, IOError):
            pass
    with open(pid_path, "w") as f:
        f.write(str(os.getpid()))
    import atexit, signal
    def _cleanup():
        try:
            if os.path.exists(pid_path): os.remove(pid_path)
        except Exception: pass
    atexit.register(_cleanup)
    def _sig(s, f): _cleanup(); raise SystemExit(0)
    signal.signal(signal.SIGTERM, _sig)
    logger.info("=" * 50)
    logger.info(f"СТАРТ | build_stats | клиент: {client_key}")
    logger.info("=" * 50)

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    write_status(client_key, {
        "operation":  "build_stats",
        "client":     client_key,
        "status":     "running",
        "started_at": started_at,
        "finished_at": None,
        "items_processed": 0,
        "error": None,
    })

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        today    = datetime.now().date()

        # текущие 7 дней: вчера минус 6 дней = последние 7 дней включая вчера
        # (сегодня может быть неполным — данные за сегодня тоже берём)
        period_to   = today
        period_from = today - timedelta(days=6)   # 7д включительно

        prev_to     = today - timedelta(days=7)
        prev_from   = today - timedelta(days=13)  # предыдущие 7д

        logger.info(f"Период текущий  (7д): {period_from} -> {period_to}")
        logger.info(f"Период прошлый  (7д): {prev_from} -> {prev_to}")

        # --- все item_id клиента у которых есть хоть какие-то данные ---
        cur = conn.execute("""
            SELECT DISTINCT item_id FROM item_stats WHERE client_key = ?
        """, (client_key,))
        item_ids = [r["item_id"] for r in cur.fetchall()]
        logger.info(f"Объявлений в базе для клиента: {len(item_ids)}")

        if not item_ids:
            logger.warning("Нет данных в item_stats — запусти сначала download.py")
            return

        # --- агрегируем 7д и пред.7д одним запросом ---
        # GROUP BY item_id + period (cur/prev через CASE)
        cur = conn.execute("""
            SELECT
                item_id,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN impressions      ELSE 0 END) AS imp_7d,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN views             ELSE 0 END) AS views_7d,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN contacts          ELSE 0 END) AS contacts_7d,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN favorites         ELSE 0 END) AS fav_7d,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN all_spend         ELSE 0 END) AS spend_7d,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN impressions      ELSE 0 END) AS imp_prev,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN views             ELSE 0 END) AS views_prev,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN contacts          ELSE 0 END) AS contacts_prev,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN favorites         ELSE 0 END) AS fav_prev,
                SUM(CASE WHEN stat_date BETWEEN ? AND ? THEN all_spend         ELSE 0 END) AS spend_prev
            FROM item_stats
            WHERE client_key = ?
              AND stat_date BETWEEN ? AND ?
            GROUP BY item_id
        """, (
            str(period_from), str(period_to),   # imp_7d
            str(period_from), str(period_to),   # views_7d
            str(period_from), str(period_to),   # contacts_7d
            str(period_from), str(period_to),   # fav_7d
            str(period_from), str(period_to),   # spend_7d
            str(prev_from),   str(prev_to),     # imp_prev
            str(prev_from),   str(prev_to),     # views_prev
            str(prev_from),   str(prev_to),     # contacts_prev
            str(prev_from),   str(prev_to),     # fav_prev
            str(prev_from),   str(prev_to),     # spend_prev
            client_key,
            str(prev_from),   str(period_to),   # WHERE диапазон — оба периода
        ))
        rows = cur.fetchall()
        logger.info(f"Строк после SQL-агрегации: {len(rows)}")

        # --- для формулы ставки нужны мин/макс/prev_bid из Sheets ---
        # пока их нет в базе — используем заглушки (0)
        # они будут заполнены после первой выгрузки export_stats.py
        # и скрипт нужно будет перезапустить
        # TODO: после написания export_stats.py — читать из отдельной таблицы sheet_data

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        upsert_rows = []

        for r in rows:
            imp_7d      = r["imp_7d"]      or 0
            views_7d    = r["views_7d"]    or 0
            contacts_7d = r["contacts_7d"] or 0
            fav_7d      = r["fav_7d"]      or 0
            spend_7d    = r["spend_7d"]    or 0.0

            imp_prev      = r["imp_prev"]      or 0
            views_prev    = r["views_prev"]    or 0
            contacts_prev = r["contacts_prev"] or 0
            fav_prev      = r["fav_prev"]      or 0
            spend_prev    = r["spend_prev"]    or 0.0

            # метрики 7д
            ctr_7d = safe_div(views_7d,    imp_7d)
            cvr_7d = safe_div(contacts_7d, views_7d)
            cpl_7d = safe_div(spend_7d,    contacts_7d)
            cpv_7d = safe_div(spend_7d,    views_7d)

            # метрики пред.7д
            ctr_prev = safe_div(views_prev,    imp_prev)
            cvr_prev = safe_div(contacts_prev, views_prev)
            cpl_prev = safe_div(spend_prev,    contacts_prev)
            cpv_prev = safe_div(spend_prev,    views_prev)

            # дельты
            d_contacts = delta_pct(contacts_7d, contacts_prev)
            d_cpl      = delta_pct(cpl_7d,      cpl_prev)

            # ставка из кода (мин/макс/prev_bid пока 0 — обновится после export_stats)
            bid_code   = calc_bid(
                minn=0, maxx=0, prev_bid=0,
                ctr=ctr_7d, clicks=views_7d, leads=contacts_7d
            )
            limit_code = None  # лимит пока не считаем — нет формулы

            upsert_rows.append((
                r["item_id"], client_key,
                str(period_from), str(period_to),
                imp_7d, views_7d, contacts_7d, fav_7d, round(spend_7d, 2),
                ctr_7d, cvr_7d, cpl_7d, cpv_7d,
                imp_prev, views_prev, contacts_prev, fav_prev, round(spend_prev, 2),
                ctr_prev, cvr_prev, cpl_prev, cpv_prev,
                d_contacts, d_cpl,
                bid_code, limit_code,
                now,
            ))

        # --- INSERT OR REPLACE в current_stats ---
        conn.executemany("""
            INSERT OR REPLACE INTO current_stats (
                item_id, client_key,
                period_from, period_to,
                impressions_7d, views_7d, contacts_7d, favorites_7d, spend_7d,
                ctr_7d, cvr_7d, cpl_7d, cpv_7d,
                impressions_prev, views_prev, contacts_prev, favorites_prev, spend_prev,
                ctr_prev, cvr_prev, cpl_prev, cpv_prev,
                delta_contacts, delta_cpl,
                bid_code, limit_code,
                updated_at
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, upsert_rows)
        conn.commit()

        # --- sync_log ---
        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = {
            "items_processed": len(upsert_rows),
            "period_from":     str(period_from),
            "period_to":       str(period_to),
        }
        conn.execute("""
            INSERT INTO sync_log (client_key, operation, started_at, finished_at, status, result_json)
            VALUES (?, 'build_stats', ?, ?, 'done', ?)
        """, (client_key, started_at, finished_at, json.dumps(result, ensure_ascii=False)))
        conn.commit()

        write_status(client_key, {
            "operation":       "build_stats",
            "client":          client_key,
            "status":          "done",
            "started_at":      started_at,
            "finished_at":     finished_at,
            "items_processed": len(upsert_rows),
            "period_from":     str(period_from),
            "period_to":       str(period_to),
            "error":           None,
        })

        logger.info("=" * 50)
        logger.info(f"ГОТОВО | build_stats | {client_key}")
        logger.info(f"  обработано объявлений: {len(upsert_rows)}")
        logger.info(f"  период: {period_from} -> {period_to}")
        logger.info("=" * 50)

        # --- вывод примера для проверки ---
        logger.info("Топ-5 объявлений по контактам:")
        cur = conn.execute("""
            SELECT item_id, views_7d, contacts_7d, spend_7d,
                   ctr_7d, cpl_7d, views_prev, contacts_prev
            FROM current_stats
            WHERE client_key = ?
            ORDER BY contacts_7d DESC
            LIMIT 5
        """, (client_key,))
        for row in cur.fetchall():
            logger.info(
                f"  item={row[0]:12d} | "
                f"views={row[1]:4d} contacts={row[2]:3d} spend={row[3]:7.2f}₽ | "
                f"CTR={row[4]:.3f} CPL={row[5]:7.2f}₽ | "
                f"prev: views={row[6]:4d} contacts={row[7]:3d}"
            )

    except Exception as e:
        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.error(f"Ошибка: {e}", exc_info=True)
        write_status(client_key, {
            "operation":       "build_stats",
            "client":          client_key,
            "status":          "error",
            "started_at":      started_at,
            "finished_at":     finished_at,
            "items_processed": 0,
            "error":           str(e),
        })

    finally:
        conn.close()
        # снимаем lock
        try:
            if os.path.exists(pid_path): os.remove(pid_path)
        except Exception: pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Расчёт сводной аналитики Авито")
    ap.add_argument("--client", required=True, help="Ключ клиента из clients.json (например: evg)")
    args = ap.parse_args()

    run(args.client)