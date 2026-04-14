"""
scripts/export_stats.py — Выгрузка current_stats → Google Sheets + пересчёт bid_code.

Использование:
    python -m stavmnog.scripts.export_stats --client=evg
"""

import argparse
import json
import os
import re
import sqlite3
import time
from datetime import datetime
from venv import logger

from shared.config import DB_PATH, DB_CONF
from shared.google_sheets import get_gspread_client
from shared.logger import get_logger

from stavmnog.config import get_client_config, STATUS_DIR, LOG_DIR, SHEET_COLUMNS
from stavmnog.utils.pid_lock import acquire_lock, release_lock
from stavmnog.utils.formulas import calc_bid, safe_float, col_letter, norm_avito_id, build_header_index

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]
BATCH_SIZE = 500


def _write_status(client_key, data):
    os.makedirs(STATUS_DIR, exist_ok=True)
    path = os.path.join(STATUS_DIR, f"export_{client_key}.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _extract_sheet_id(raw: str) -> str:
    """
    Извлекает ID таблицы из полного URL или возвращает как есть.

    Фикс бага: apply_bids передавал полный URL вместо ID.
    """
    if "/d/" in raw:
        m = re.search(r"/d/([a-zA-Z0-9_-]+)", raw)
        if m:
            return m.group(1)
    return raw.strip()


def _open_sheet(sheet_id_raw: str, sheet_name: str, key_file: str):
    """Открывает worksheet, извлекая ID из URL если нужно."""
    import gspread
    from google.oauth2.service_account import Credentials

    sheet_id = _extract_sheet_id(sheet_id_raw)
    creds = Credentials.from_service_account_file(key_file, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    return sh.worksheet(sheet_name)


def _ensure_columns(ws, logger):
    """Проверяет и добавляет колонки V–AA если их нет."""
    header = ws.row_values(1)
    needed = {
        "% пред": COL["pct_prev"], "ц лид пред": COL["cpl_prev"],
        "клики пред": COL["clicks_prev"], "лид пред": COL["leads_prev"],
        "ставка из кода": COL["bid_code"], "лимит из кода": COL["limit_code"],
        "Статус": COL["Статус"], "Сообщение": COL["Сообщение"],
        "!Применил": COL["Применил"], "дата применения": COL["дата_прим"],
    }
    updates = []
    for name, col_idx in needed.items():
        while len(header) < col_idx:
            header.append("")
        if header[col_idx - 1].strip() == "":
            updates.append({"range": f"{col_letter(col_idx)}1", "values": [[name]]})
            header[col_idx - 1] = name
            logger.info(f"  Добавлена колонка {col_letter(col_idx)}: {name}")
    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")


def _batch_update_with_retry(ws, updates, logger, max_retries=3):
    """batch_update с retry при 429/503."""
    for i in range(0, len(updates), BATCH_SIZE):
        chunk = updates[i:i + BATCH_SIZE]
        for attempt in range(max_retries):
            try:
                ws.batch_update(chunk, value_input_option="USER_ENTERED")
                logger.info(f"  Записано {min(i + BATCH_SIZE, len(updates))} / {len(updates)}")
                break
            except Exception as e:
                err_str = str(e)
                if "429" in err_str or "503" in err_str:
                    wait = 60 * (attempt + 1)
                    logger.warning(f"  Sheets API limit — ждём {wait} сек (попытка {attempt + 1})")
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
    logger.info("=" * 50)

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_status(client_key, {
        "operation": "export_stats", "client": client_key,
        "status": "running", "started_at": started_at,
        "finished_at": None, "rows_written": 0, "error": None,
    })

    cfg = get_client_config(client_key)
    key_file = os.path.join(DB_CONF, cfg["google_key_file"])

    logger.info(f"Подключение к Google Sheets: {cfg['sheet_id']}")
    ws = _open_sheet(cfg["sheet_id"], cfg["sheet_bids"], key_file)

    hdr_idx = build_header_index(ws, SHEET_COLUMNS, logger=logger)

    all_rows = ws.get_all_values()
    header = all_rows[0] if all_rows else []
    data_rows = all_rows[1:]
    logger.info(f"Строк в листе: {len(data_rows)}")

    avito_col_0 = hdr_idx["AvitoId"] - 1    # ← вместо COL["AvitoId"] - 1
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




    logger.info(f"AvitoId в листе: валидных={len(sheet_index)}, пустых/битых={len(sheet_rows_bad)}")
    if sheet_rows_bad[:5]:
        logger.info(f"Примеры битых: {sheet_rows_bad[:5]}")

    # logger.info(f"AvitoId в листе: {len(sheet_index)}")
    logger.info(f"Примеры ключей (первые 5): {list(sheet_index.keys())[:5]}")

    # Данные из SQLite
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        stats = conn.execute("""
            SELECT item_id, ctr_7d, cpl_7d, views_7d, contacts_7d,
                ctr_prev, cpl_prev, views_prev, contacts_prev,
                bid_code, limit_code
            FROM current_stats WHERE client_key = ?
        """, (client_key,)).fetchall()

        # Строим словарь БД по нормализованному ключу
        db_by_id = {}
        for s in stats:
            nid = norm_avito_id(s["item_id"])
            if nid:
                db_by_id[nid] = s

        logger.info(f"Аналитика в БД: всего={len(stats)}, с валидным id={len(db_by_id)}")

        # Диагностика пересечений
        sheet_ids = set(sheet_index.keys())
        db_ids = set(db_by_id.keys())
        in_both = sheet_ids & db_ids
        only_db = db_ids - sheet_ids
        only_sheet = sheet_ids - db_ids
        logger.info(f"В обоих: {len(in_both)} | только БД: {len(only_db)} | только лист: {len(only_sheet)}")
        if only_db:
            logger.info(f"В БД есть, в листе НЕТ (первые 5): {list(only_db)[:5]}")
        if only_sheet:
            logger.info(f"В листе есть, в БД НЕТ (первые 5): {list(only_sheet)[:5]}")

        minn_col  = hdr_idx["мин"] - 1
        maxx_col  = hdr_idx["макс"] - 1
        koret_col = hdr_idx["корект"] - 1

        updates = []
        bid_code_updates = []
        rows_written = 0
        rows_zeroed = 0  # не нашли в БД — ставим нули
        rows_marked = 0  # невалидный AvitoId — ставим маркер

        def cell(col_name, sheet_row, value):
            return {"range": f"{col_letter(hdr_idx[col_name])}{sheet_row}",
                    "values": [[value]]}

        # 1) Проходим по всем валидным строкам листа
        for avito_id, sheet_row in sheet_index.items():
            data_idx = sheet_row - 2
            sdr = data_rows[data_idx] if data_idx < len(data_rows) else []

            minn = safe_float(sdr[minn_col] if minn_col < len(sdr) else 0)
            maxx = safe_float(sdr[maxx_col] if maxx_col < len(sdr) else 0)
            prev_bid = safe_float(sdr[koret_col] if koret_col < len(sdr) else 0)

            s = db_by_id.get(avito_id)

            if s is not None:
                # Нормальный случай — есть статистика
                ctr_7d = s["ctr_7d"] or 0
                views_7d = s["views_7d"] or 0
                contacts_7d = s["contacts_7d"] or 0
                bid_code = calc_bid(minn, maxx, prev_bid, ctr_7d, views_7d, contacts_7d)
                bid_code_updates.append((bid_code, int(avito_id), client_key))

                updates += [
                    cell("%",          sheet_row, round(ctr_7d, 4)),
                    cell("ц лид",      sheet_row, round(s["cpl_7d"] or 0, 2)),
                    cell("клики",      sheet_row, views_7d),
                    cell("лид",        sheet_row, contacts_7d),
                    cell("% пред",     sheet_row, round(s["ctr_prev"] or 0, 4)),
                    cell("ц лид пред", sheet_row, round(s["cpl_prev"] or 0, 2)),
                    cell("клики пред", sheet_row, s["views_prev"] or 0),
                    cell("лид пред",   sheet_row, s["contacts_prev"] or 0),
                    cell("ставка из кода", sheet_row, bid_code),
                    cell("лимит из кода",  sheet_row,
                        s["limit_code"] if s["limit_code"] is not None else ""),
                ]
                rows_written += 1
            else:
                # AvitoId есть в листе, но в БД нет данных — ставим нули
                bid_code = calc_bid(minn, maxx, prev_bid, 0, 0, 0)
                updates += [
                    cell("%",          sheet_row, 0),
                    cell("ц лид",      sheet_row, 0),
                    cell("клики",      sheet_row, 0),
                    cell("лид",        sheet_row, 0),
                    cell("% пред",     sheet_row, 0),
                    cell("ц лид пред", sheet_row, 0),
                    cell("клики пред", sheet_row, 0),
                    cell("лид пред",   sheet_row, 0),
                    cell("ставка из кода", sheet_row, bid_code),
                    cell("лимит из кода",  sheet_row, ""),
                ]
                rows_zeroed += 1

        # 2) Строки с битым/пустым AvitoId — помечаем в колонке Сообщение
        for sheet_row, raw in sheet_rows_bad:
            updates.append(cell("Сообщение", sheet_row, "нет AvitoId"))
            rows_marked += 1

        logger.info(
            f"Запись: c данными={rows_written}, нулями={rows_zeroed}, "
            f"без AvitoId={rows_marked}, всего ячеек={len(updates)}"
        )
        _batch_update_with_retry(ws, updates, logger)       

        if bid_code_updates:
            conn.executemany("""
                UPDATE current_stats SET bid_code = ?, updated_at = datetime('now')
                WHERE item_id = ? AND client_key = ?
            """, bid_code_updates)
            conn.commit()
            logger.info(f"bid_code обновлён в БД: {len(bid_code_updates)}")

        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""
            INSERT INTO sync_log (client_key, operation, started_at, finished_at, status, result_json)
            VALUES (?, 'export_stats', ?, ?, 'done', ?)
        """, (client_key, started_at, finished_at, json.dumps({"rows_written": rows_written})))
        conn.commit()

        _write_status(client_key, {
            "operation": "export_stats", "client": client_key,
            "status": "done", "started_at": started_at,
            "finished_at": finished_at, "rows_written": rows_written, "error": None,
        })
        logger.info(f"ГОТОВО | export_stats | {client_key} | {rows_written} строк")

    except Exception as e:
        logger.error(f"Ошибка: {e}", exc_info=True)
        _write_status(client_key, {
            "operation": "export_stats", "client": client_key,
            "status": "error", "started_at": started_at,
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "rows_written": 0, "error": str(e),
        })

    finally:
        conn.close()
        release_lock("export_stats", client_key)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", required=True)
    args = ap.parse_args()
    run(args.client)

