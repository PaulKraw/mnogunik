#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/apply_bids.py — Применение ставок из Google Sheets через Avito API.

Использование:
    python scripts/apply_bids.py --client=coffee
    python scripts/apply_bids.py --client=coffee --respect-date
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from typing import Optional

import pandas as pd

from shared.config import DB_CONF
from shared.avito_api import get_avito_token, avito_get, avito_post

from stavmnog.config import get_client_config, STATUS_DIR, LOG_DIR, SHEET_COLUMNS
from stavmnog.utils.formulas import col_letter, norm_avito_id, build_header_index, open_worksheet
from stavmnog.utils.pid_lock import acquire_lock, release_lock

WINDOW_CHUNK = 1000
BATCH_SIZE = 20
AVITO_DELAY_SEC = 0.5   # между запросами к одному клиенту


# ═══════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════

class Logger:
    """Простой логгер: в stdout (nohup пишет в файл) и с timestamp."""
    def __init__(self, prefix=""):
        self.prefix = prefix

    def info(self, msg):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"{ts} {self.prefix} {msg}", flush=True)

    def warn(self, msg):
        self.info(f"⚠ {msg}")

    def err(self, msg):
        self.info(f"✗ {msg}")


# ═══════════════════════════════════════════
# УТИЛИТЫ
# ═══════════════════════════════════════════

def _today_dm():
    return datetime.now().strftime("%d.%m")


def _write_json(path, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _get_last_row(ws, anchor_col):
    vals = ws.col_values(anchor_col)
    i = len(vals) - 1
    while i >= 0 and (vals[i] is None or str(vals[i]).strip() == ""):
        i -= 1
    return max(1, i + 1)


def _batch_update_retry(ws, updates, logger, max_retries=3):
    for attempt in range(max_retries):
        try:
            ws.batch_update(updates, value_input_option="USER_ENTERED")
            return
        except Exception as e:
            err = str(e)
            if ("429" in err or "503" in err) and attempt < max_retries - 1:
                wait = 30 * (attempt + 1)
                logger.warn(f"Sheets API limit, ждём {wait}с")
                time.sleep(wait)
            else:
                raise


# ═══════════════════════════════════════════
# AVITO API
# ═══════════════════════════════════════════

def get_bid_limits(token, item_id, logger):
    url = f"https://api.avito.ru/cpxpromo/1/getBids/{item_id}"
    headers = {"Authorization": f"Bearer {token}"}
    r = avito_get(url, headers)
    if r is None or r.status_code != 200:
        return None
    try:
        j = r.json()
        manual = j.get("manual") or {}
        return {
            "minBidPenny": manual.get("minBidPenny"),
            "maxBidPenny": manual.get("maxBidPenny"),
        }
    except Exception:
        return None


def set_manual_bid(token, ad_id, bid_penny, limit_penny=None):
    url = "https://api.avito.ru/cpxpromo/1/setManual"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"itemID": ad_id, "bidPenny": bid_penny, "actionTypeID": 5}
    if limit_penny is not None:
        payload["limitPenny"] = limit_penny
    response = avito_post(url, headers, payload)
    if response is None:
        return 500, {"error": "No response"}
    try:
        return response.status_code, response.json()
    except Exception:
        return response.status_code, {"error": "Invalid JSON"}


def parse_limit_penny(raw):
    if pd.isna(raw):
        return None
    s = str(raw).strip()
    if s == "" or s.lower() == "none":
        return None
    try:
        v = float(s.replace(",", "."))
    except ValueError:
        return None
    if v <= 0:
        return None
    return int(round(v)) * 100   # целые рубли в копейках


def parse_bid_penny(raw):
    """Ставка из Sheets → копейки. Округляем до рубля (API требует)."""
    if raw is None or str(raw).strip() == "":
        return None
    try:
        v = float(str(raw).replace(",", "."))
    except ValueError:
        return None
    if v <= 0:
        return None
    return int(round(v)) * 100   # целые рубли в копейках


# ═══════════════════════════════════════════
# СТОП-ФЛАГ
# ═══════════════════════════════════════════

def stop_requested(client_key):
    return os.path.exists(os.path.join(STATUS_DIR, f"stop_bids_{client_key}.flag"))


def clear_stop_flag(client_key):
    flag = os.path.join(STATUS_DIR, f"stop_bids_{client_key}.flag")
    if os.path.exists(flag):
        try:
            os.remove(flag)
        except Exception:
            pass


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", required=True)
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    ap.add_argument("--ignore-date", action="store_true", default=True)
    ap.add_argument("--respect-date", dest="ignore_date", action="store_false")
    args = ap.parse_args()

    client = args.client
    log = Logger(f"[bids:{client}]")
    log.info("=" * 50)
    log.info(f"СТАРТ | ignore_date={args.ignore_date}")

    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(STATUS_DIR, exist_ok=True)
    clear_stop_flag(client)

    # PID-lock чтобы kill.php работал
    if not acquire_lock("bids", client):
        log.warn("Уже запущен — выход")
        return

    status_path = os.path.join(STATUS_DIR, f"bids_{client}.json")
    failed_path = os.path.join(STATUS_DIR, f"failed_ids_{client}_{datetime.now().strftime('%Y-%m-%d')}.json")
    _write_json(failed_path, [])

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = {
        "operation": "apply_bids",
        "client": client,
        "status": "running",
        "started_at": started_at,
        "finished_at": None,
        "today": _today_dm(),
        "total": 0,
        "overall": {"taken": 0, "done": 0, "ok": 0, "err": 0, "skip": 0},
        "error": None,
    }
    _write_json(status_path, status)

    try:
        cfg = get_client_config(client)
        key_file = os.path.join(DB_CONF, cfg["google_key_file"])

        token = get_avito_token(cfg["client_id"], cfg["client_secret"])
        log.info("Токен Авито получен")

        ws = open_worksheet(cfg["sheet_id"], cfg["sheet_bids"], key_file, logger=log)
        hdr_idx = build_header_index(ws, SHEET_COLUMNS, logger=log)

        last_row = _get_last_row(ws, hdr_idx["AvitoId"])
        if last_row < 2:
            log.info("Нет данных в листе")
            status["status"] = "done"
            status["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _write_json(status_path, status)
            return

        status["total"] = last_row - 1
        _write_json(status_path, status)

        def col_0(name):
            return hdr_idx[name] - 1

        start_row = 2
        today_dm = _today_dm()

        while start_row <= last_row:
            if stop_requested(client):
                log.info("СТОП — флаг из панели")
                status["status"] = "stopped"
                status["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _write_json(status_path, status)
                clear_stop_flag(client)
                return

            end_row = min(last_row, start_row + WINDOW_CHUNK - 1)
            last_col_letter = col_letter(max(hdr_idx.values()))
            rows = ws.get(f"A{start_row}:{last_col_letter}{end_row}")

            # Читаем по именам колонок
            recs = []
            for i, row_vals in enumerate(rows):
                def v(name):
                    idx = col_0(name)
                    return row_vals[idx] if idx < len(row_vals) else ""
                recs.append({
                    "__row__":         start_row + i,
                    "AvitoId":         v("AvitoId"),
                    "Ставка":          v("Ставка"),
                    "Лимит":           v("Лимит"),
                    "!Применил":       v("!Применил"),
                    "дата применения": v("дата применения"),
                })

            # Фильтрация
            candidates = []
            clear_updates = []
            stats_f = {"total": 0, "already_da": 0, "same_date": 0,
                       "no_avito": 0, "no_bid": 0, "to_do": 0}

            for r in recs:
                stats_f["total"] += 1
                rn = r["__row__"]
                prim = str(r["!Применил"]).strip().lower()
                dat = str(r["дата применения"]).strip()

                avito_norm = norm_avito_id(r["AvitoId"])
                if not avito_norm:
                    stats_f["no_avito"] += 1
                    continue
                r["AvitoId"] = avito_norm

                bid_penny = parse_bid_penny(r["Ставка"])
                if bid_penny is None:
                    stats_f["no_bid"] += 1
                    continue
                r["_bid_penny"] = bid_penny

                if args.ignore_date:
                    if prim == "да" or dat:
                        clear_updates += [
                            {"range": f"{col_letter(hdr_idx['!Применил'])}{rn}", "values": [[""]]},
                            {"range": f"{col_letter(hdr_idx['дата применения'])}{rn}", "values": [[""]]},
                        ]
                    stats_f["to_do"] += 1
                    candidates.append(r)
                    continue

                if prim == "да":
                    stats_f["already_da"] += 1
                    continue
                if dat == today_dm:
                    stats_f["same_date"] += 1
                    continue

                stats_f["to_do"] += 1
                candidates.append(r)

            log.info(f"Окно {start_row}-{end_row}: {stats_f}")

            if clear_updates:
                _batch_update_retry(ws, clear_updates, log)

            if not candidates:
                start_row = end_row + 1
                continue

            # Обработка батчами
            for batch in _chunked(candidates, args.batch_size):
                if stop_requested(client):
                    log.info("СТОП — флаг из панели (внутри батча)")
                    status["status"] = "stopped"
                    status["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    _write_json(status_path, status)
                    clear_stop_flag(client)
                    return

                updates = []
                failed_ids = []
                batch_ok = batch_err = 0

                for rec in batch:
                    rn = rec["__row__"]
                    ad_id = int(rec["AvitoId"])
                    bid_penny = rec["_bid_penny"]

                    # Коррекция по лимитам API (если получилось запросить)
                    limits = get_bid_limits(token, ad_id, log)
                    final_bid = bid_penny
                    if limits is not None:
                        min_b = limits.get("minBidPenny")
                        max_b = limits.get("maxBidPenny")
                        if isinstance(min_b, int) and final_bid < min_b:
                            final_bid = int(round(min_b / 100)) * 100
                        if isinstance(max_b, int) and final_bid > max_b:
                            final_bid = int(round(max_b / 100)) * 100

                    limit_penny = parse_limit_penny(rec.get("Лимит"))
                    code, resp = set_manual_bid(token, ad_id, final_bid, limit_penny)

                    if code == 200:
                        updates += [
                            {"range": f"{col_letter(hdr_idx['Статус'])}{rn}", "values": [["OK"]]},
                            {"range": f"{col_letter(hdr_idx['Сообщение'])}{rn}", "values": [[f"{final_bid//100}₽"]]},
                            {"range": f"{col_letter(hdr_idx['!Применил'])}{rn}", "values": [["да"]]},
                            {"range": f"{col_letter(hdr_idx['дата применения'])}{rn}", "values": [[today_dm]]},
                        ]
                        batch_ok += 1
                    else:
                        msg = resp.get("message") if isinstance(resp, dict) else str(resp)
                        msg = str(msg)[:200] if msg else f"HTTP {code}"
                        log.err(f"row={rn} id={ad_id} bid={final_bid}: {msg}")
                        updates += [
                            {"range": f"{col_letter(hdr_idx['Статус'])}{rn}", "values": [["ERR"]]},
                            {"range": f"{col_letter(hdr_idx['Сообщение'])}{rn}", "values": [[msg]]},
                            {"range": f"{col_letter(hdr_idx['дата применения'])}{rn}", "values": [[today_dm]]},
                        ]
                        batch_err += 1
                        failed_ids.append(str(ad_id))

                    time.sleep(AVITO_DELAY_SEC)

                # Запись результатов
                if updates:
                    _batch_update_retry(ws, updates, log)

                if failed_ids:
                    try:
                        cur = []
                        if os.path.exists(failed_path):
                            with open(failed_path, "r", encoding="utf-8") as f:
                                cur = json.load(f)
                        cur.extend(failed_ids)
                        _write_json(failed_path, cur)
                    except Exception:
                        pass

                status["overall"]["taken"] += len(batch)
                status["overall"]["done"] += len(batch)
                status["overall"]["ok"] += batch_ok
                status["overall"]["err"] += batch_err
                _write_json(status_path, status)
                log.info(f"Батч: OK={batch_ok} ERR={batch_err} | всего OK={status['overall']['ok']} ERR={status['overall']['err']}")

            start_row = end_row + 1

        status["status"] = "done"
        status["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _write_json(status_path, status)

        ov = status["overall"]
        log.info(f"ГОТОВО | taken={ov['taken']} OK={ov['ok']} ERR={ov['err']}")

    except Exception as e:
        log.err(f"Исключение: {e}")
        import traceback
        traceback.print_exc()
        status["status"] = "error"
        status["error"] = str(e)[:500]
        status["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _write_json(status_path, status)
        sys.exit(1)

    finally:
        release_lock("bids", client)


if __name__ == "__main__":
    main()