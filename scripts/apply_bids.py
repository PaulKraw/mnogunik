"""
scripts/apply_bids.py — Применение ставок из Google Sheets через Avito API.

Читает лист «Ставки», для каждой строки где !Применил != «да»
отправляет setManual (bid, dailyLimit), пишет результат в Sheets.

Использование:
    python -m stavmnog.scripts.apply_bids --client=evg
"""

import argparse
import json
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from shared.config import ROOT_DIR, DB_PATH, DB_CONF
from shared.avito_api import get_avito_token, avito_get, avito_post
from shared.logger import write_log

from stavmnog.config import get_client_config, STATUS_DIR, LOG_DIR, STAVMNOG_DIR
from stavmnog.utils.pid_lock import acquire_lock, release_lock
from stavmnog.utils.formulas import col_letter

# ═══════════════════════════════════════════
# КОНСТАНТЫ
# ═══════════════════════════════════════════

WINDOW_CHUNK = 1000
BATCH_SIZE = 20
AVITO_DELAY_SEC = 0.25

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


# ═══════════════════════════════════════════
# УТИЛИТЫ
# ═══════════════════════════════════════════

def _today_dm() -> str:
    return datetime.now().strftime("%d.%m")


def _write_json(path: str, obj: dict) -> None:
    """Атомарная запись JSON (фикс бага с отсутствующей папкой)."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _extract_sheet_id(raw: str) -> str:
    """Извлекает ID из полного URL Google Sheets."""
    import re
    if "/d/" in raw:
        m = re.search(r"/d/([a-zA-Z0-9_-]+)", raw)
        if m:
            return m.group(1)
    return raw.strip()


# ═══════════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════════

def _open_worksheet(sheet_id_raw: str, gid: int, cred_file: str):
    import gspread
    from google.oauth2.service_account import Credentials

    sheet_id = _extract_sheet_id(sheet_id_raw)
    creds = Credentials.from_service_account_file(cred_file, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.get_worksheet_by_id(gid) if hasattr(sh, "get_worksheet_by_id") else None
    if ws is None:
        ws = sh.get_worksheet(0)
    return ws


def _get_header(ws) -> list:
    return [h.strip() for h in ws.row_values(1)]


def _ensure_columns(ws, header, required):
    changed = False
    for col in required:
        if col not in header:
            header.append(col)
            changed = True
    if changed:
        last = col_letter(len(header))
        ws.update(f"A1:{last}1", [header])
    hdr = _get_header(ws)
    idx = {name: i + 1 for i, name in enumerate(hdr)}
    return hdr, idx


def _get_last_row(ws, anchor_col: int) -> int:
    vals = ws.col_values(anchor_col)
    i = len(vals) - 1
    while i >= 0 and (vals[i] is None or str(vals[i]).strip() == ""):
        i -= 1
    return max(1, i + 1)


def _batch_update_retry(ws, updates, max_retries=3):
    """batch_update с retry при 429/503."""
    for attempt in range(max_retries):
        try:
            ws.batch_update(updates, value_input_option="USER_ENTERED")
            return
        except Exception as e:
            err = str(e)
            if "429" in err or "503" in err:
                wait = 60 * (attempt + 1)
                write_log(f"  Sheets API limit — ждём {wait}с (попытка {attempt + 1})")
                time.sleep(wait)
            else:
                raise


# ═══════════════════════════════════════════
# AVITO API — СТАВКИ
# ═══════════════════════════════════════════

def get_bid_limits(token: str, item_id: int) -> Optional[dict]:
    """Получает лимиты ставки для объявления."""
    url = f"https://api.avito.ru/cpxpromo/1/getBids/{item_id}"
    headers = {"Authorization": f"Bearer {token}"}
    r = avito_get(url, headers)
    if r is None or r.status_code != 200:
        return None
    try:
        j = r.json()
    except Exception:
        return None
    manual = j.get("manual") or {}
    return {
        "minBidPenny": manual.get("minBidPenny"),
        "maxBidPenny": manual.get("maxBidPenny"),
        "recBidPenny": manual.get("recBidPenny"),
    }


def set_manual_bid(token: str, ad_id: int, bid_penny: int,
                   action_type_id: int = 5, limit_penny: Optional[int] = None):
    """Отправляет setManual запрос к Avito API."""
    url = "https://api.avito.ru/cpxpromo/1/setManual"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"itemID": ad_id, "bidPenny": bid_penny, "actionTypeID": action_type_id}
    if limit_penny is not None:
        payload["limitPenny"] = limit_penny

    response = avito_post(url, headers, payload)
    if response is None:
        return 500, {"error": "No response"}
    try:
        return response.status_code, response.json()
    except Exception:
        return response.status_code, {"error": "Invalid JSON"}


def parse_limit_penny(raw) -> Optional[int]:
    """Парсит лимит из ячейки Sheets → копейки."""
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
    return int(round(v * 100))


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════

def main():
    write_log(f"[apply_bids] ПРЕ СТАРТ | ")

    ap = argparse.ArgumentParser(description="Применение ставок Авито из Google Sheets")
    ap.add_argument("--client", required=True, help="Ключ клиента из clients.json")
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    ap.add_argument("--ignore-date", action="store_true",
                    help="Очистить 'дата применения' перед стартом")
    args = ap.parse_args()


    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(STATUS_DIR, exist_ok=True)

    # if not acquire_lock("bids", args.client):
    #     return

    cfg = get_client_config(args.client)
    sheet_id = cfg["sheet_id"]
    gid = cfg.get("sheet_bids_gid", 0)
    key_file = os.path.join(DB_CONF, cfg["google_key_file"])

    
    write_log(f"[apply_bids] key_file | {key_file}")


    #вот где же он не отчитывается но работает? 
    status_path = os.path.join(STATUS_DIR, f"bids_{args.client}.json")
    failed_path = os.path.join(
        STATUS_DIR, f"failed_ids_{args.client}_{datetime.now().strftime('%Y-%m-%d')}.json"
    )
    _write_json(failed_path, [])

    status = {
        "ts": time.time(),
        "today": _today_dm(),
        "status": "running",
        "total": 0,
        "overall": {"taken": 0, "done": 0, "ok": 0, "err": 0, "skip": 0},
        "by_client": {},
        "current": {},
    }
    _write_json(status_path, status)

    try:
        write_log(f"[apply_bids] СТАРТ | {args.client}")

        ws = _open_worksheet(sheet_id, gid, key_file)
        header = _get_header(ws)
        need = ["Статус", "Сообщение", "!Применил", "дата применения",
                "name", "name_csv", "AvitoId", "Ставка"]
        header, hdr_idx = _ensure_columns(ws, header, need)

        anchor = hdr_idx.get("name") or hdr_idx.get("AvitoId") or 1
        last_row = _get_last_row(ws, anchor)
        if last_row < 2:
            write_log("[apply_bids] Нет данных в листе")
            status["status"] = "done"
            _write_json(status_path, status)
            release_lock("bids", args.client)
            return

        status["total"] = last_row - 1
        _write_json(status_path, status)

        token_cache = {}
        start_row = 2
        today_dm = _today_dm()

        while start_row <= last_row:
            end_row = min(last_row, start_row + WINDOW_CHUNK - 1)
            last_col = col_letter(len(header))
            rows = ws.get(f"A{start_row}:{last_col}{end_row}")

            recs = []
            for i, row_vals in enumerate(rows):
                rec = {"__row__": start_row + i}
                for ci, col_name in enumerate(header):
                    rec[col_name] = row_vals[ci] if ci < len(row_vals) else ""
                recs.append(rec)

            # Фильтрация
            candidates = []
            for r in recs:
                if str(r.get("!Применил", "")).strip().lower() == "да":
                    continue
                if str(r.get("дата применения", "")).strip() == today_dm:
                    continue
                avito_raw = str(r.get("AvitoId", "")).strip()
                bid_raw = str(r.get("Ставка", "")).strip()
                if not avito_raw or avito_raw.lower() == "none" or not bid_raw:
                    candidates.append((r, "skip"))
                    continue
                candidates.append((r, "do"))

            if not candidates:
                start_row = end_row + 1
                continue

            # Батчи
            for batch in _chunked(candidates, args.batch_size):
                # Стоп-флаг
                stop_flag = os.path.join(STATUS_DIR, f"stop_bids_{args.client}.flag")
                if os.path.exists(stop_flag):
                    write_log("[apply_bids] СТОП — флаг из панели")
                    status["status"] = "stopped"
                    _write_json(status_path, status)
                    release_lock("bids", args.client)
                    return

                groups = defaultdict(list)
                for rec, kind in batch:
                    key = (str(rec.get("name", "")).strip(),
                           str(rec.get("name_csv", "")).strip())
                    groups[key].append((rec, kind))

                updates = []
                failed_ids = []
                batch_ok = batch_err = batch_skip = batch_done = 0

                for (name, name_csv), items in groups.items():
                    # API-ключи клиента в априори есть в конфиге. Если их нет, то это баг в конфиге, а не в данных листа, и лучше падать с ошибкой, чем молча пропускать.


                    # Токен (кеш на батч) я придумал что лучше
                    token = token_cache.get((name, name_csv))
                    if not token:
                        token = get_avito_token(cfg["client_id"], cfg["client_secret"])
                        
                        token_cache[(name, name_csv)] = token

                    for rec, kind in items:
                        rn = rec["__row__"]
                        if kind == "skip":
                            updates += [
                                {"range": f"{col_letter(hdr_idx['Статус'])}{rn}", "values": [["SKIP"]]},
                                {"range": f"{col_letter(hdr_idx['Сообщение'])}{rn}", "values": [["empty"]]},
                                {"range": f"{col_letter(hdr_idx['дата применения'])}{rn}", "values": [[today_dm]]},
                            ]
                            batch_skip += 1
                            batch_done += 1
                            continue

                        avito_id_s = str(rec.get("AvitoId", "")).strip()
                        bid_s = str(rec.get("Ставка", "")).strip()
                        try:
                            bid_penny = int(round(float(bid_s.replace(",", ".")) * 100))
                        except Exception:
                            updates += [
                                {"range": f"{col_letter(hdr_idx['Статус'])}{rn}", "values": [["ERR"]]},
                                {"range": f"{col_letter(hdr_idx['Сообщение'])}{rn}", "values": [[f"bad bid '{bid_s}'"]]},
                                {"range": f"{col_letter(hdr_idx['дата применения'])}{rn}", "values": [[today_dm]]},
                            ]
                            batch_err += 1
                            batch_done += 1
                            continue

                        limit_penny = parse_limit_penny(rec.get("Лимит"))

                        # Лимиты API
                        ad_id = int(float(avito_id_s))
                        limits = get_bid_limits(token, ad_id)
                        final_bid = bid_penny
                        if limits is not None:
                            min_b = limits.get("minBidPenny")
                            max_b = limits.get("maxBidPenny")
                            if isinstance(min_b, int) and final_bid < min_b:
                                final_bid = min_b
                            if isinstance(max_b, int) and final_bid > max_b:
                                final_bid = max_b

                        # Отправка
                        code, resp = set_manual_bid(token, ad_id, final_bid, limit_penny=limit_penny)

                        if code == 200:
                            updates += [
                                {"range": f"{col_letter(hdr_idx['Статус'])}{rn}", "values": [["OK"]]},
                                {"range": f"{col_letter(hdr_idx['Сообщение'])}{rn}", "values": [["-"]]},
                                {"range": f"{col_letter(hdr_idx['!Применил'])}{rn}", "values": [["да"]]},
                                {"range": f"{col_letter(hdr_idx['дата применения'])}{rn}", "values": [[today_dm]]},
                            ]
                            batch_ok += 1
                        else:
                            msg = resp.get("message") if isinstance(resp, dict) else str(resp)
                            updates += [
                                {"range": f"{col_letter(hdr_idx['Статус'])}{rn}", "values": [["ERR"]]},
                                {"range": f"{col_letter(hdr_idx['Сообщение'])}{rn}", "values": [[str(msg)[:200]]]},
                                {"range": f"{col_letter(hdr_idx['дата применения'])}{rn}", "values": [[today_dm]]},
                            ]
                            batch_err += 1
                            failed_ids.append(avito_id_s)

                        batch_done += 1
                        time.sleep(AVITO_DELAY_SEC)

                # Запись в Sheets
                if updates:
                    _batch_update_retry(ws, updates)

                # Сохранение ошибок
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

                # Обновление статуса
                status["overall"]["taken"] += len(batch)
                status["overall"]["done"] += batch_done
                status["overall"]["ok"] += batch_ok
                status["overall"]["err"] += batch_err
                status["overall"]["skip"] += batch_skip
                status["current"] = {}
                status["ts"] = time.time()
                _write_json(status_path, status)

            start_row = end_row + 1

        # Готово
        status["status"] = "done"
        status["current"] = {"state": "done"}
        status["ts"] = time.time()
        _write_json(status_path, status)

        write_log(
            f"[apply_bids] ГОТОВО | {args.client} | "
            f"OK:{status['overall']['ok']} ERR:{status['overall']['err']}"
        )

    except Exception as e:
        write_log(f"[apply_bids] ОШИБКА | {args.client} | {e}")
        status["status"] = "error"
        status["error"] = str(e)[:500]
        _write_json(status_path, status)

    finally:
        release_lock("bids", args.client)


if __name__ == "__main__":
    main()
