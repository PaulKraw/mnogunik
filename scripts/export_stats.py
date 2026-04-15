#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/export_stats.py — Выгрузка current_stats → Google Sheets + пересчёт bid_code.
"""

import argparse
import json
import os
import sqlite3
import time
from datetime import datetime

from shared.config import DB_PATH, DB_CONF
from shared.logger import get_logger

from stavmnog.config import get_client_config, STATUS_DIR, LOG_DIR, SHEET_COLUMNS
from stavmnog.utils.pid_lock import acquire_lock, release_lock
from stavmnog.utils.formulas import (
    calc_bid, safe_float, col_letter, norm_avito_id,
    build_header_index, open_worksheet,
)

BATCH_SIZE = 500


def _write_status(client_key, data):
    os.makedirs(STATUS_DIR, exist_ok=True)
    path = os.path.join(STATUS_DIR, f"export_{client_key}.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _batch_update_with_retry(ws, updates, logger, max_retries=3):
    for i in range(0, len(updates), BATCH_SIZE):
        chunk = updates[i:i + BATCH_SIZE]
        for attempt in range(max_retries):
            try:
                ws.batch_update(chunk, value_input_option="USER_ENTERED")
                logger.info(f"  Записано {min(i + BATCH_SIZE, len(updates))} / {len(updates)}")
                break
            except Exception as e:
                err_str = str(e)
                if ("429" in err_str or "503" in err_str) and attempt < max_retries - 1:
                    wait = 30 * (attempt + 1)
                    logger.warning(f"  Sheets API limit — ждём {wait}с")
                    time.sleep(wait)
                else:
                    raise


def run(client_key: str):
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = get_logger(
        f"export_{client_key}",
        os.path.join(LOG_DIR, f"export_stats_{client_key}.log"),
    )

    if not acquire_lock("export_stats", client_key, logger):
        return

    logger.info("=" * 50)
    logger.info(f"СТАРТ | export_stats | {client_key}")

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_status(client_key, {
        "operation": "export_stats", "client": client_key,
        "status": "running", "started_at": started_at,
        "finished_at": None, "rows_written": 0, "error": None,
    })

    try:
        cfg = get_client_config(client_key)
        key_file = os.path.join(DB_CONF, cfg["google_key_file"])

        ws = open_worksheet(cfg["sheet_id"], cfg["sheet_bids"], key_file, logger=logger)
        hdr_idx = build_header_index(ws, SHEET_COLUMNS, logger=logger)

        all_rows = ws.get_all_values()
        data_rows = all_rows[1:] if all_rows else []
        logger.info(f"Строк в листе: {len(data_rows)}")

        avito_col_0 = hdr_idx["AvitoId"] - 1
        sheet_index = {}
        sheet_rows_bad = []
        for i, row in enumerate(data_rows):
            raw = row[avito_col_0] if avito_col_0 < len(row) else ""
            clean = norm_avito_id(raw)
            sheet_row = i + 2
            if not clean:
                sheet_rows_bad.append((sheet_row, raw))
                continue
            sheet_index[clean] = sheet_row

        logger.info(f"AvitoId в листе: валидных={len(sheet_index)}, битых={len(sheet_rows_bad)}")

        # Из БД
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        stats = conn.execute("""
            SELECT item_id,
                   ctr_7d, cpl_7d, views_7d, contacts_7d, spend_7d,
                   ctr_prev, cpl_prev, views_prev, contacts_prev, spend_prev,
                   bid_code, limit_code
            FROM current_stats WHERE client_key = ?
        """, (client_key,)).fetchall()

        db_by_id = {norm_avito_id(s["item_id"]): s for s in stats if norm_avito_id(s["item_id"])}
        logger.info(f"Аналитика в БД: {len(db_by_id)}")

        in_both = set(sheet_index) & set(db_by_id)
        only_sheet = set(sheet_index) - set(db_by_id)
        logger.info(f"Совпало: {len(in_both)} | только в листе: {len(only_sheet)}")

        minn_col  = hdr_idx["мин"] - 1
        maxx_col  = hdr_idx["макс"] - 1
        koret_col = hdr_idx["корект"] - 1

        def cell(col_name, sheet_row, value):
            return {"range": f"{col_letter(hdr_idx[col_name])}{sheet_row}", "values": [[value]]}

        updates = []
        bid_code_updates = []
        rows_written = rows_zeroed = 0

        for avito_id, sheet_row in sheet_index.items():
            sdr = data_rows[sheet_row - 2] if sheet_row - 2 < len(data_rows) else []

            minn     = safe_float(sdr[minn_col]  if minn_col  < len(sdr) else 0)
            maxx     = safe_float(sdr[maxx_col]  if maxx_col  < len(sdr) else 0)
            prev_bid = safe_float(sdr[koret_col] if koret_col < len(sdr) else 0)

            s = db_by_id.get(avito_id)
            if s is not None:
                bid_code = calc_bid(
                    minn, maxx, prev_bid,
                    s["ctr_7d"] or 0, s["views_7d"] or 0, s["contacts_7d"] or 0,
                )
                bid_code_updates.append((bid_code, int(avito_id), client_key))

                updates += [
                    cell("%",             sheet_row, round(s["ctr_7d"] or 0, 4)),
                    cell("ц лид",         sheet_row, round(s["cpl_7d"] or 0, 2)),
                    cell("клики",         sheet_row, s["views_7d"] or 0),
                    cell("лид",           sheet_row, s["contacts_7d"] or 0),
                    cell("расход 7д",     sheet_row, round(s["spend_7d"] or 0, 2)),
                    cell("% пред",        sheet_row, round(s["ctr_prev"] or 0, 4)),
                    cell("ц лид пред",    sheet_row, round(s["cpl_prev"] or 0, 2)),
                    cell("клики пред",    sheet_row, s["views_prev"] or 0),
                    cell("лид пред",      sheet_row, s["contacts_prev"] or 0),
                    cell("расход пред",   sheet_row, round(s["spend_prev"] or 0, 2)),
                    cell("ставка из кода", sheet_row, bid_code),
                    cell("лимит из кода",  sheet_row, s["limit_code"] if s["limit_code"] is not None else ""),
                ]
                rows_written += 1
            else:
                bid_code = calc_bid(minn, maxx, prev_bid, 0, 0, 0)
                updates += [
                    cell("%",             sheet_row, 0),
                    cell("ц лид",         sheet_row, 0),
                    cell("клики",         sheet_row, 0),
                    cell("лид",           sheet_row, 0),
                    cell("расход 7д",     sheet_row, 0),
                    cell("% пред",        sheet_row, 0),
                    cell("ц лид пред",    sheet_row, 0),
                    cell("клики пред",    sheet_row, 0),
                    cell("лид пред",      sheet_row, 0),
                    cell("расход пред",   sheet_row, 0),
                    cell("ставка из кода", sheet_row, bid_code),
                    cell("лимит из кода",  sheet_row, ""),
                ]
                rows_zeroed += 1

        for sheet_row, raw in sheet_rows_bad:
            updates.append(cell("Сообщение", sheet_row, "нет AvitoId"))

        logger.info(f"Запись: с данными={rows_written}, нулями={rows_zeroed}, ячеек={len(updates)}")
        _batch_update_with_retry(ws, updates, logger)

        if bid_code_updates:
            conn.executemany("""
                UPDATE current_stats SET bid_code = ?, updated_at = datetime('now')
                WHERE item_id = ? AND client_key = ?
            """, bid_code_updates)
            conn.commit()

        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _write_status(client_key, {
            "operation": "export_stats", "client": client_key,
            "status": "done", "started_at": started_at,
            "finished_at": finished_at, "rows_written": rows_written, "error": None,
        })
        logger.info(f"ГОТОВО | {client_key} | {rows_written} строк")
        conn.close()

    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        _write_status(client_key, {
            "operation": "export_stats", "client": client_key,
            "status": "error", "started_at": started_at,
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "rows_written": 0, "error": str(e),
        })

    finally:
        release_lock("export_stats", client_key)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", required=True)
    args = ap.parse_args()
    run(args.client)