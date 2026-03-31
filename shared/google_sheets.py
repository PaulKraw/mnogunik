"""
shared/google_sheets.py — Работа с Google Sheets для всех модулей.

Заменяет:
- generator: utils/google_sheets.py (download_csv, make_csv_url)
- pricecraft: utils/sheets_writer.py (upload_df_to_sheet)
- pricecraft: utils/data_fetcher.py (download_csv часть)
- stavmnog: gspread-загрузку

Использование:
    from shared.google_sheets import download_csv, upload_df_to_sheet, get_gspread_client
"""

import json
import os
import re
import time
from typing import Optional

import pandas as pd
import requests

from shared.config import GOOGLE_CREDENTIALS_PATH
from shared.logger import write_log


# ═══════════════════════════════════════════
# СКАЧИВАНИЕ
# ═══════════════════════════════════════════

def make_csv_url(sheet_url: str) -> str:
    """
    Преобразует URL Google Sheets в прямую ссылку на CSV-экспорт.

    Args:
        sheet_url: URL вида https://docs.google.com/.../edit?gid=123

    Returns:
        URL для скачивания CSV.

    Raises:
        ValueError: Если не удалось извлечь ID или gid.
    """
    try:
        start = sheet_url.index("/d/") + 3
        end = sheet_url.index("/", start)
        sid = sheet_url[start:end]
    except ValueError:
        raise ValueError(f"Невозможно определить ID таблицы: {sheet_url}")

    gid: Optional[str] = None
    for sep in ("?gid=", "#gid="):
        if sep in sheet_url:
            gid = sheet_url.split(sep)[1].split("&")[0]
            break

    if not gid:
        raise ValueError(f"Невозможно определить gid: {sheet_url}")

    return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"


def download_csv(url: str, save_path: str, timeout: int = 10) -> str:
    """
    Скачивает CSV по URL и сохраняет на диск.

    Args:
        url: URL для скачивания.
        save_path: Путь для сохранения.
        timeout: Таймаут в секундах.

    Returns:
        Путь к сохранённому файлу.
    """
    try:
        resp = requests.get(url, timeout=timeout, verify=False)
        resp.raise_for_status()

        if not resp.content:
            write_log("Ошибка: файл пустой!")
            raise SystemExit(1)

        os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(resp.content)

        write_log(f"✅ Файл загружен: {save_path}")
        return save_path

    except requests.exceptions.RequestException as e:
        write_log(f"Ошибка загрузки: {e}")
        if os.path.exists(save_path):
            write_log(f"📂 Используется локальный: {save_path}")
            return save_path
        write_log("❌ Файл не найден локально")
        raise SystemExit(1)


def download_sheet_csv(
    base_url: str, gid: int, name: str, save_dir: str = "csv"
) -> pd.DataFrame:
    """
    Скачивает лист Google Sheets как CSV → DataFrame.

    Аналог pricecraft.down_respons().

    Args:
        base_url: Базовый URL таблицы (без /export).
        gid: GID листа.
        name: Имя файла для сохранения.
        save_dir: Папка.

    Returns:
        DataFrame.
    """
    os.makedirs(save_dir, exist_ok=True)
    filepath = os.path.join(save_dir, name)
    url = f"{base_url}/export?format=csv&gid={gid}"

    resp = requests.get(url, verify=False)
    resp.raise_for_status()

    with open(filepath, "wb") as f:
        f.write(resp.content)

    df = pd.read_csv(filepath)
    # Форматирование целых чисел (убираем .0)
    df = df.apply(
        lambda col: col.map(
            lambda x: f"{x:.0f}"
            if pd.notna(x) and isinstance(x, float) and x == int(x)
            else x
        )
    )
    return df


# ═══════════════════════════════════════════
# GSPREAD КЛИЕНТ
# ═══════════════════════════════════════════

def get_gspread_client(credentials_path: Optional[str] = None):
    """
    Возвращает авторизованный gspread-клиент.

    Args:
        credentials_path: Путь к credentials.json. Если None — из .env.

    Returns:
        gspread.Client.
    """
    import gspread

    path = credentials_path or GOOGLE_CREDENTIALS_PATH
    if not path:
        raise FileNotFoundError(
            "GOOGLE_CREDENTIALS_PATH не задан в .env и не передан аргументом"
        )

    with open(path) as f:
        creds = json.load(f)

    return gspread.service_account_from_dict(creds)


# ═══════════════════════════════════════════
# ВЫГРУЗКА В SHEETS
# ═══════════════════════════════════════════

def upload_df_to_sheet(
    ws,
    df: pd.DataFrame,
    clear_first: bool = True,
    sheet_label: str = "лист",
) -> None:
    """
    Очищает лист Google Sheets и записывает DataFrame.

    Заменяет 6 копий одинакового блока из pricecraft.

    Args:
        ws: gspread Worksheet.
        df: DataFrame.
        clear_first: Очистить перед записью.
        sheet_label: Имя для логов.
    """
    # Очистка
    df_clean = df.copy().fillna("")
    df_clean = df_clean.applymap(
        lambda x: x.item() if hasattr(x, "item") else x
    )
    df_clean = df_clean.reset_index(drop=True)

    rows = [df_clean.columns.tolist()] + df_clean.astype(str).values.tolist()
    non_empty = [any(c.strip() for c in r) for r in rows[1:]]

    write_log(f"[{sheet_label}] Строк: {len(df_clean)}, непустых: {sum(non_empty)}")

    if clear_first:
        ws.clear()
        time.sleep(0.5)

    if len(df_clean) > 0 and sum(non_empty) > 0:
        ws.update(range_name="A1", values=rows, value_input_option="USER_ENTERED")
        write_log(f"[{sheet_label}] ✅ Записано {len(df_clean)} строк")
    else:
        write_log(f"[{sheet_label}] ⚠️ Нет данных для записи")
