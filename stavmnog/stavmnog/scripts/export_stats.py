"""
export_stats.py — выгрузка current_stats → Google Sheets + чтение мин/макс/корект → пересчёт bid_code

Использование:
    python scripts/export_stats.py --client=evg
"""

import argparse
import json
import logging
import math
import os
import sqlite3
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Пути
# ---------------------------------------------------------------------------
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "clients.json")
DB_PATH     = os.path.join(BASE_DIR, "data", "avito.db")
STATUS_DIR  = os.path.join(BASE_DIR, "status")
LOG_DIR     = os.path.join(BASE_DIR, "logs")

# ---------------------------------------------------------------------------
# Колонки листа Ставки
# Буква → индекс (1-based) для gspread
# ---------------------------------------------------------------------------
COL = {
    "Id":           1,   # A
    "AvitoId":      2,   # B
    "Ставка":       3,   # C
    "Лимит":        4,   # D
    "name":         5,   # E
    "name_csv":     6,   # F
    "лим_клик":     7,   # G
    "корект":       8,   # H  ← prev_bid для формулы
    "кол":          9,   # I
    "пр_став":      10,  # J
    "мин":          11,  # K  ← min_bid для формулы
    "макс":         12,  # L  ← max_bid для формулы
    "цен":          13,  # M
    "pct_7d":       14,  # N  — CTR текущей недели  (пишем)
    "cpl_7d":       15,  # O  — цена лида тек. недели (пишем)
    "clicks_7d":    16,  # P  — просмотры тек. недели (пишем)
    "leads_7d":     17,  # Q  — контакты тек. недели (пишем)
    "pct_prev":     18,  # R  — CTR прош. недели (пишем)
    "cpl_prev":     19,  # S  — цена лида прош. недели (пишем)
    "clicks_prev":  20,  # T  — просмотры прош. недели (пишем)
    "leads_prev":   21,  # U  — контакты прош. недели (пишем)
    "bid_code":     22,  # V  — ставка из кода (пишем)
    "limit_code":   23,  # W  — лимит из кода (пишем)
    "Статус":       24,  # X
    "Сообщение":    25,  # Y
    "Применил":     26,  # Z
    "дата_прим":    27,  # AA
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------
def setup_logger(client_key: str) -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"export_stats_{client_key}.log")
    logger = logging.getLogger(f"export.stats.{client_key}")
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
    path = os.path.join(STATUS_DIR, f"export_{client_key}.json")
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Индекс колонки (1-based) → буква A1-нотации
# ---------------------------------------------------------------------------
def col_letter(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# ---------------------------------------------------------------------------
# Формула ставки (та же что в build_stats.py)
# ---------------------------------------------------------------------------
def calc_bid(minn: float, maxx: float, prev_bid: float,
             ctr: float, clicks: int, leads: int) -> float:
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
# Безопасное float из ячейки Sheets
# ---------------------------------------------------------------------------
def safe_float(val) -> float:
    if val is None or str(val).strip() == "":
        return 0.0
    try:
        return float(str(val).replace(",", ".").replace(" ", ""))
    except ValueError:
        return 0.0


# ---------------------------------------------------------------------------
# Открыть worksheet по имени
# ---------------------------------------------------------------------------
def open_sheet(sheet_id: str, sheet_name: str, key_file: str):
    creds = Credentials.from_service_account_file(key_file, scopes=SCOPES)
    gc    = gspread.authorize(creds)
    sh    = gc.open_by_key(sheet_id)
    ws    = sh.worksheet(sheet_name)
    return ws


# ---------------------------------------------------------------------------
# Убедиться что колонки V–AA существуют (добавить если нет)
# ---------------------------------------------------------------------------
def ensure_columns(ws, logger):
    header = ws.row_values(1)
    needed = {
        "% пред":        COL["pct_prev"],
        "ц лид пред":    COL["cpl_prev"],
        "клики пред":    COL["clicks_prev"],
        "лид пред":      COL["leads_prev"],
        "ставка из кода": COL["bid_code"],
        "лимит из кода":  COL["limit_code"],
        "Статус":         COL["Статус"],
        "Сообщение":      COL["Сообщение"],
        "!Применил":      COL["Применил"],
        "дата применения": COL["дата_прим"],
    }
    updates = []
    for name, col_idx in needed.items():
        # расширяем header если короче
        while len(header) < col_idx:
            header.append("")
        if header[col_idx - 1].strip() == "":
            updates.append({
                "range":  f"{col_letter(col_idx)}1",
                "values": [[name]],
            })
            header[col_idx - 1] = name
            logger.info(f"  Добавлена колонка {col_letter(col_idx)}: {name}")

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
    return header


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------
def run(client_key: str):
    logger = setup_logger(client_key)

    # защита от двойного запуска через PID-файл
    os.makedirs(STATUS_DIR, exist_ok=True)
    pid_path = os.path.join(STATUS_DIR, f"export_stats_{client_key}.pid")
    if os.path.exists(pid_path):
        try:
            old_pid = int(open(pid_path).read().strip())
            try:
                os.kill(old_pid, 0)
                logger.warning(f"export_stats {client_key} уже запущен (pid={old_pid}) — выходим")
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
    logger.info(f"СТАРТ | export_stats | клиент: {client_key}")
    logger.info("=" * 50)

    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    write_status(client_key, {
        "operation":    "export_stats",
        "client":       client_key,
        "status":       "running",
        "started_at":   started_at,
        "finished_at":  None,
        "rows_written": 0,
        "error":        None,
    })

    # --- конфиг ---
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        clients = json.load(f)

    if client_key not in clients:
        raise ValueError(f"Клиент '{client_key}' не найден в clients.json")

    cfg      = clients[client_key]
    sheet_id = cfg["sheet_id"]
    sheet_name = cfg["sheet_bids"]
    key_file = os.path.join(BASE_DIR, cfg["google_key_file"])

    # --- открываем лист ---
    logger.info(f"Подключаемся к Google Sheets...")
    logger.info(f"  таблица : {sheet_id}")
    logger.info(f"  лист    : {sheet_name}")
    ws = open_sheet(sheet_id, sheet_name, key_file)

    # --- проверяем/добавляем колонки ---
    logger.info("Проверяем заголовки колонок листа...")
    ensure_columns(ws, logger)

    # --- читаем весь лист ---
    logger.info("Читаем все строки листа...")
    all_rows = ws.get_all_values()   # список списков, строка 0 = заголовок
    header   = all_rows[0] if all_rows else []
    data_rows = all_rows[1:]         # строки с данными (индекс 0 = строка 2 листа)

    logger.info(f"Строк в листе (без заголовка): {len(data_rows)}")

    # --- строим индекс AvitoId → номер строки листа (2-based) ---
    avito_col_idx = COL["AvitoId"] - 1   # 0-based для списка
    sheet_index = {}   # avito_id (str) → row_number (2-based)
    for i, row in enumerate(data_rows):
        avito_raw = row[avito_col_idx] if avito_col_idx < len(row) else ""
        avito_raw = str(avito_raw).strip()
        if avito_raw and avito_raw.lower() not in ("", "none", "avitoid"):
            # убираем .0 если пришло как float
            try:
                avito_clean = str(int(float(avito_raw)))
            except ValueError:
                avito_clean = avito_raw
            sheet_index[avito_clean] = i + 2   # +1 заголовок +1 0-based

    logger.info(f"Найдено AvitoId в листе: {len(sheet_index)}")

    # --- читаем current_stats из базы ---
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.execute("""
            SELECT item_id,
                   ctr_7d, cpl_7d, views_7d, contacts_7d,
                   ctr_prev, cpl_prev, views_prev, contacts_prev,
                   bid_code, limit_code
            FROM current_stats
            WHERE client_key = ?
        """, (client_key,))
        stats_rows = cur.fetchall()
        logger.info(f"Строк аналитики в БД для клиента: {len(stats_rows)}")

        # --- строим батч обновлений для Sheets ---
        updates     = []
        rows_written = 0

        # параллельно читаем K, L, H из листа для пересчёта bid_code
        minn_col   = COL["мин"]     - 1  # 0-based
        maxx_col   = COL["макс"]    - 1
        koret_col  = COL["корект"]  - 1

        bid_code_updates = []  # (item_id, bid_code) для записи в базу

        for s in stats_rows:
            item_id_str = str(s["item_id"])

            if item_id_str not in sheet_index:
                continue   # объявление не найдено в листе — пропускаем

            sheet_row = sheet_index[item_id_str]
            data_idx  = sheet_row - 2   # обратно в 0-based индекс data_rows

            # читаем мин/макс/корет из листа
            sheet_data_row = data_rows[data_idx] if data_idx < len(data_rows) else []
            minn     = safe_float(sheet_data_row[minn_col]  if minn_col  < len(sheet_data_row) else 0)
            maxx     = safe_float(sheet_data_row[maxx_col]  if maxx_col  < len(sheet_data_row) else 0)
            prev_bid = safe_float(sheet_data_row[koret_col] if koret_col < len(sheet_data_row) else 0)

            # пересчитываем bid_code с реальными мин/макс/корет
            bid_code = calc_bid(
                minn=minn, maxx=maxx, prev_bid=prev_bid,
                ctr=s["ctr_7d"] or 0,
                clicks=s["views_7d"] or 0,
                leads=s["contacts_7d"] or 0,
            )
            bid_code_updates.append((bid_code, s["item_id"], client_key))

            # формируем обновления ячеек
            def cell(col_idx, value):
                return {
                    "range":  f"{col_letter(col_idx)}{sheet_row}",
                    "values": [[value]],
                }

            updates += [
                cell(COL["pct_7d"],     round(s["ctr_7d"]      or 0, 4)),
                cell(COL["cpl_7d"],     round(s["cpl_7d"]      or 0, 2)),
                cell(COL["clicks_7d"],  s["views_7d"]           or 0),
                cell(COL["leads_7d"],   s["contacts_7d"]        or 0),
                cell(COL["pct_prev"],   round(s["ctr_prev"]    or 0, 4)),
                cell(COL["cpl_prev"],   round(s["cpl_prev"]    or 0, 2)),
                cell(COL["clicks_prev"],s["views_prev"]         or 0),
                cell(COL["leads_prev"], s["contacts_prev"]      or 0),
                cell(COL["bid_code"],   bid_code),
                cell(COL["limit_code"], s["limit_code"] if s["limit_code"] is not None else ""),
            ]
            rows_written += 1

        logger.info(f"Совпало объявлений (БД + лист): {rows_written} -> запись...")
        logger.info(f"Всего ячеек для обновления: {len(updates)}")

        # --- пишем в Sheets батчами по 500 обновлений ---
        BATCH = 500
        for i in range(0, len(updates), BATCH):
            chunk = updates[i:i + BATCH]
            ws.batch_update(chunk, value_input_option="USER_ENTERED")
            logger.info(f"  Записано {min(i + BATCH, len(updates))} / {len(updates)}")

        # --- обновляем bid_code в current_stats в базе ---
        if bid_code_updates:
            conn.executemany("""
                UPDATE current_stats SET bid_code = ?, updated_at = datetime('now')
                WHERE item_id = ? AND client_key = ?
            """, bid_code_updates)
            conn.commit()
            logger.info(f"Обновлён bid_code в базе для {len(bid_code_updates)} объявлений")

        # --- sync_log ---
        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = {"rows_written": rows_written}
        conn.execute("""
            INSERT INTO sync_log (client_key, operation, started_at, finished_at, status, result_json)
            VALUES (?, 'export_stats', ?, ?, 'done', ?)
        """, (client_key, started_at, finished_at, json.dumps(result)))
        conn.commit()

        write_status(client_key, {
            "operation":    "export_stats",
            "client":       client_key,
            "status":       "done",
            "started_at":   started_at,
            "finished_at":  finished_at,
            "rows_written": rows_written,
            "error":        None,
        })

        logger.info("=" * 50)
        logger.info(f"ГОТОВО | export_stats | {client_key}")
        logger.info(f"  записано строк в Sheets: {rows_written}")
        logger.info("=" * 50)

    except Exception as e:
        finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.error(f"Ошибка: {e}", exc_info=True)
        write_status(client_key, {
            "operation":    "export_stats",
            "client":       client_key,
            "status":       "error",
            "started_at":   started_at,
            "finished_at":  finished_at,
            "rows_written": 0,
            "error":        str(e),
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
    ap = argparse.ArgumentParser(description="Выгрузка аналитики в Google Sheets")
    ap.add_argument("--client", required=True, help="Ключ клиента из clients.json (например: evg)")
    args = ap.parse_args()

    run(args.client)