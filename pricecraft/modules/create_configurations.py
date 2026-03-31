#!/usr/bin/env python3
"""
modules/create_configurations.py — Создание конфигураций ПК.

Читает «шахматку» процессор×видеокарта из Google Sheets,
генерирует все комбинации с комплектующими, рассчитывает цены,
записывает результат в лист «configurations».
"""

import os
import sys
import time
import traceback

import pandas as pd

# ── Импорты проекта ──
from pricecraft.config import sheets, settings
from shared.config import SPREADSHEET_ID
from shared.logger import write_log, write_status
from shared.google_sheets import upload_df_to_sheet
from pricecraft.utils.button_status import finish
from pricecraft.modules import get_config

MODULE_NAME = "create_configurations"

# Параметры шахматки
GUID = 72304825
MIN_ROW = 14
MIN_COL = 5


def main():
    try:
        write_log(f"START {MODULE_NAME}")
        write_status("running", "Создаются конфигурации...", MODULE_NAME)

        client = sheets.get_client()
        write_log("Google Sheets client OK")

        sh = client.open_by_key(SPREADSHEET_ID)

        # Лист configurations
        try:
            ws = sh.worksheet("configurations")
        except Exception:
            ws = sh.add_worksheet(title="configurations", rows=100, cols=10)

        wsprices = sh.worksheet("Price_new")
        ws.clear()
        write_log("Листы открыты")

        # ── 1. Загрузка данных ──
        df = get_config.down_respons_main(
            settings.url_batya, GUID, "cpu_gpu.xlsx", MIN_ROW, MIN_COL, "00FFFF"
        )
        write_log("Шахматка загружена")

        df_vk_bp = get_config.down_respons(settings.url_batya, 634015161, "vk_bp.csv")
        write_log("Таблица ВК→БП загружена")

        df_price_kompl = get_config.down_respons(settings.url_batya, 406620284, "price_kompl.csv")
        df_price_kompl = df_price_kompl.dropna(subset=["ОЗОН. Вариации."])
        write_log("Комплектующие загружены")

        # ── 2. Обработка ──
        df = get_config.init_df(df)
        write_log("init_df OK")

        os.makedirs("csv", exist_ok=True)

        df_blu_out = get_config.getHashTable(df)
        write_log(f"Хеш-таблица: {df_blu_out.shape[0]} строк, {df_blu_out.shape[1]} столбцов")

        values = settings.get_values(wsprices)
        write_log("Коэффициенты загружены")

        df_final = get_config.create_table(df_blu_out, df_vk_bp, df_price_kompl, values)
        write_log(f"Финальная таблица: {df_final.shape[0]} строк")

        # ── 3. Запись в Google Sheets ──
        upload_df_to_sheet(ws, df_final, sheet_label="configurations")

        finish(MODULE_NAME)
        write_log(f"FINISH {MODULE_NAME}")
        write_status("finished", "Конфигурации созданы", MODULE_NAME)

    except Exception as e:
        tb = traceback.format_exc()
        write_log(f"ERROR: {e}")
        write_log(tb)
        write_status("error", str(e), MODULE_NAME)


if __name__ == "__main__":
    main()
