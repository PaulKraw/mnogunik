#!/usr/bin/env python3
"""
modules/generate_ozon_file.py — Генерация прайс-листа для Ozon.

Читает конфигурации, применяет маппинг параметров и наценки,
записывает результат в лист «generate_ozon_file».
"""

import time
import traceback

import pandas as pd

from pricecraft.config import sheets, settings
from shared.config import SPREADSHEET_ID
from shared.logger import write_log, write_status
from shared.google_sheets import upload_df_to_sheet
from pricecraft.utils.button_status import finish
from pricecraft.modules import get_config

MODULE_NAME = "generate_ozon_file"


def main():
    try:
        client = sheets.get_client()
        sh = client.open_by_key(SPREADSHEET_ID)

        try:
            ws = sh.worksheet("generate_ozon_file")
        except Exception:
            ws = sh.add_worksheet(title="generate_ozon_file", rows=100, cols=10)

        ws.clear()
        time.sleep(0.5)

        wsconf = sh.worksheet("configurations")
        wsprices = sh.worksheet("Price_new")
        values = settings.get_values(wsprices)

        write_log(f"[{MODULE_NAME}] Коэф. Ozon: {values['ozon']}")

        data = wsconf.get_all_values()
        df = get_config.init_conf_df(pd.DataFrame(data))

        # Маппинг параметров
        wsmapping = sh.worksheet("parameter_mapping")
        df_mapping = pd.DataFrame(wsmapping.get_all_records())

        # Комплектующие
        df_price_kompl = get_config.down_respons(settings.url_batya, 406620284, "price_kompl.csv")
        df_price_kompl = df_price_kompl.dropna(subset=["ОЗОН. Вариации."])
        write_log("Комплектующие загружены")

        # Генерация Ozon-таблицы
        df_final = get_config.create_ozon_table(df, df_price_kompl, values, df_mapping)

        # Запись
        upload_df_to_sheet(ws, df_final, sheet_label="generate_ozon_file")

        write_log(f"[{MODULE_NAME}] Завершено")
        finish(MODULE_NAME)

    except Exception as e:
        tb = traceback.format_exc()
        write_log(f"ERROR: {e}")
        write_log(tb)
        write_status("error", str(e), MODULE_NAME)


if __name__ == "__main__":
    main()
