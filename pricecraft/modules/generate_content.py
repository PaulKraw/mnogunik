#!/usr/bin/env python3
"""
modules/generate_content.py — Генерация контента для маркетплейсов (Ozon / WB).

Объединяет generate_ozon_content.py и generate_wb_content.py,
которые на 90% дублировали друг друга. Теперь один модуль с параметром marketplace.

Логика:
1. Применяет маппинг параметров для площадки
2. Записывает в лист content / content_wb
3. Создаёт задачу в cl_rows для генерации mnogunik
4. Запускает PHP-скрипт генерации
5. Ожидает результат (CSV-файл)
6. Читает Description + ImageUrls из файла → пишет в content
"""

import time
import traceback
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from pricecraft.config import sheets, settings
from shared.config import (
    SPREADSHEET_ID,
    SPREADSHEET_CLROWS_ID,
    MNOGUNIK_RUN_URL,
    GENERATOR_WEB_KEY as MNOGUNIK_RUN_KEY,
)
from shared.logger import write_log, write_status
from shared.google_sheets import upload_df_to_sheet
from pricecraft.utils.button_status import finish
from pricecraft.modules import get_config


# ═══════════════════════════════════════════
# КОНФИГУРАЦИЯ ПО ПЛОЩАДКАМ
# ═══════════════════════════════════════════

MARKETPLACE_CONFIG = {
    "ozon": {
        "sheet_name": "content",
        "module_name": "generate_ozon_content",
        "mappings": ["ozon", "ozon_cont"],
        "desc_col": "Description_ozon",
        "img_col": "ImageUrls_ozon",
    },
    "wb": {
        "sheet_name": "content_wb",
        "module_name": "generate_wb_content",
        "mappings": ["wb", "wb_cont"],
        "desc_col": "Description_wb",
        "img_col": "ImageUrls_wb",
    },
}


# ═══════════════════════════════════════════
# ОСНОВНАЯ ЛОГИКА
# ═══════════════════════════════════════════

def _get_sheet_gid(sh, sheet_name: str) -> Optional[str]:
    """Находит GID листа по имени."""
    try:
        for ws in sh.worksheets():
            if ws.title == sheet_name:
                return str(ws.id)
    except Exception as e:
        write_log(f"Ошибка получения GID: {e}")
    return None


def _create_task(
    client, sh_clrows, marketplace: str, num_ads: int, file_price_url: str
) -> int:
    """Создаёт задачу генерации в cl_rows и возвращает ID."""
    try:
        ws = sh_clrows.worksheet("Лист1")
    except Exception:
        ws = sh_clrows.add_worksheet(title="Лист1", rows=1000, cols=20)
        ws.append_row([
            "id", "run", "file_price", "name_csv", "name", "m",
            "num_ads", "date_f", "address_to_append", "status",
        ])
        time.sleep(1)

    time.sleep(0.5)

    # Следующий ID
    try:
        tasks = ws.get_all_records()
        next_id = max((int(t.get("m", 0)) for t in tasks if t.get("m")), default=0) + 1
    except Exception:
        next_id = 1

    # Деактивируем старые задачи
    try:
        all_data = ws.get_all_values()
        time.sleep(1)
        for i in range(1, len(all_data)):
            all_data[i][1] = "0"
        ws.update("A1", all_data)
        time.sleep(1)
        write_log("Все задачи обновлены (run=0)")
    except Exception as e:
        write_log(f"Ошибка обновления задач: {e}")

    # Новая задача
    date_f = datetime.now().strftime("%Y-%m-%d")
    new_task = [
        next_id, 1, "", "sborpk", marketplace, num_ads,
        file_price_url, date_f, "https://mnogunik.ru/outfile/",
    ]
    time.sleep(0.5)
    ws.append_row(new_task)
    write_log(f"Задача ID={next_id} создана")

    return next_id


def _launch_mnogunik() -> bool:
    """Запускает PHP-скрипт генерации mnogunik."""
    try:
        write_log(f"Запуск mnogunik: {MNOGUNIK_RUN_URL}")
        resp = requests.get(
            MNOGUNIK_RUN_URL,
            params={"key": MNOGUNIK_RUN_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            write_log(f"Mnogunik запущен: {resp.text[:100]}")
            return True
        write_log(f"Ошибка запуска: {resp.status_code}")
    except requests.RequestException as e:
        write_log(f"Ошибка подключения: {e}")
    return False


def _wait_for_file(
    name: str, marketplace: str, date_f: str, num_ads: int,
    max_wait: int = 3600, interval: int = 240,
) -> Optional[str]:
    """Ожидает появление CSV-файла генерации."""
    filename = f"{name}_{marketplace}_{date_f}_{num_ads}.csv"
    urls = [
        f"https://mnogunik.ru/proj/{name}/{filename}",
        f"https://mnogunik.ru/outfile/{filename}",
    ]

    write_log(f"Ожидаем: {urls[0]}")
    start = time.time()

    while time.time() - start < max_wait:
        for url in urls:
            try:
                resp = requests.head(url, timeout=10)
                if resp.status_code == 200:
                    write_log(f"Файл найден: {url}")
                    return url
            except Exception:
                pass

        elapsed = int(time.time() - start)
        write_log(f"Ждём {interval} сек... (прошло {elapsed} сек)")
        time.sleep(interval)

    write_log("Файл не создан в отведённое время")
    return None


def _update_content_sheet(
    ws, df_result: pd.DataFrame, desc_col: str, img_col: str
) -> None:
    """Обновляет лист content данными из сгенерированного файла."""
    SOURCE_COLS = ["Description", "ImageUrls"]
    TARGET_COLS = [desc_col, img_col]

    if "articul" not in df_result.columns:
        write_log("ОШИБКА: нет колонки 'articul' в файле")
        return

    # Маппинг articul → данные
    file_data = {}
    for _, row in df_result.iterrows():
        art = str(row["articul"]).strip()
        if art and art.lower() != "nan":
            file_data[art] = {
                "Description": row.get("Description", ""),
                "ImageUrls": row.get("ImageUrls", ""),
            }

    write_log(f"Артикулов из файла: {len(file_data)}")

    # Текущий content
    try:
        content_data = ws.get_all_values()
        time.sleep(1)
        if content_data:
            df_current = pd.DataFrame(content_data[1:], columns=content_data[0])
        else:
            df_current = pd.DataFrame()
    except Exception:
        df_current = pd.DataFrame()

    # Добавляем целевые колонки
    for col in TARGET_COLS:
        if col not in df_current.columns:
            df_current[col] = ""

    # Обновляем
    updated = 0
    for idx, row in df_current.iterrows():
        art = str(row.get("articul", "")).strip()
        if art in file_data:
            df_current.at[idx, desc_col] = file_data[art].get("Description", "")
            df_current.at[idx, img_col] = file_data[art].get("ImageUrls", "")
            updated += 1
        elif art:
            df_current.at[idx, desc_col] = ""
            df_current.at[idx, img_col] = ""

    write_log(f"Обновлено: {updated}")

    # Порядок колонок
    cols = df_current.columns.tolist()
    for col in reversed(TARGET_COLS):
        if col in cols:
            cols.remove(col)
        cols.insert(1, col)
    df_current = df_current[cols]

    upload_df_to_sheet(ws, df_current, sheet_label="content")


# ═══════════════════════════════════════════
# ENTRY POINTS
# ═══════════════════════════════════════════

def run(marketplace: str) -> None:
    """
    Основная функция генерации контента.

    Args:
        marketplace: 'ozon' или 'wb'.
    """
    cfg = MARKETPLACE_CONFIG[marketplace]
    module_name = cfg["module_name"]

    try:
        write_status("running", f"Генерация контента {marketplace}...", module_name)

        client = sheets.get_client()
        sh = client.open_by_key(SPREADSHEET_ID)

        try:
            ws = sh.worksheet(cfg["sheet_name"])
        except Exception:
            ws = sh.add_worksheet(title=cfg["sheet_name"], rows=100, cols=10)

        wsconf = sh.worksheet("configurations")
        wsprices = sh.worksheet("Price_new")
        values = settings.get_values(wsprices)
        time.sleep(0.5)

        write_log(f"[{module_name}] Коэффициент: {values.get(marketplace, '?')}")

        data = wsconf.get_all_values()
        df = get_config.init_conf_df(pd.DataFrame(data))

        # Маппинг
        wsmapping = sh.worksheet("parameter_mapping")
        df_mapping = pd.DataFrame(wsmapping.get_all_records())

        for mp in cfg["mappings"]:
            df = get_config.apply_marketplace_mapping(df, df_mapping, mp)

        # Запись маппинга
        upload_df_to_sheet(ws, df, sheet_label=cfg["sheet_name"])

        # Ссылка на лист
        gid = _get_sheet_gid(sh, cfg["sheet_name"])
        file_price_url = (
            f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit?gid={gid}"
            if gid
            else f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit"
        )

        # Задача генерации
        sh_clrows = client.open_by_key(SPREADSHEET_CLROWS_ID)
        last_mp = cfg["mappings"][-1]
        _create_task(client, sh_clrows, last_mp, len(df), file_price_url)

        # Запуск mnogunik
        if not _launch_mnogunik():
            return

        # Ожидание файла
        date_f = datetime.now().strftime("%Y-%m-%d")
        file_url = _wait_for_file("sborpk", last_mp, date_f, len(df))

        if not file_url:
            return

        # Чтение и обновление контента
        try:
            df_result = pd.read_csv(file_url, dtype=str)
            write_log(f"Файл загружен: {len(df_result)} строк")
            _update_content_sheet(ws, df_result, cfg["desc_col"], cfg["img_col"])
        except Exception as e:
            write_log(f"Ошибка обработки файла: {e}")

        finish(module_name)
        write_status("finished", "Контент создан", module_name)
        write_log(f"[{module_name}] Завершено")

    except Exception as e:
        tb = traceback.format_exc()
        write_log(f"ERROR: {e}")
        write_log(tb)
        write_status("error", str(e), module_name)


# ═══════════════════════════════════════════
# ОТДЕЛЬНЫЕ ТОЧКИ ВХОДА (для обратной совместимости с PHP)
# ═══════════════════════════════════════════

def main_ozon():
    """Точка входа для generate_ozon_content."""
    run("ozon")


def main_wb():
    """Точка входа для generate_wb_content."""
    run("wb")
