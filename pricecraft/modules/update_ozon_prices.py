#!/usr/bin/env python3
"""
modules/update_ozon_prices.py — Обновление цен на Ozon через API.

Читает артикулы и цены из листа generate_ozon_file,
формирует запросы к Ozon API и отправляет пакетами.
"""

import time
import traceback

import pandas as pd
import requests

from pricecraft.config import sheets, settings
from shared.config import SPREADSHEET_ID
from shared.avito_api import get_ozon_headers, OZON_PRICE_API_URL
from shared.logger import write_log, write_status
from shared.google_sheets import upload_df_to_sheet
from pricecraft.utils.button_status import finish
from pricecraft.modules import get_config

MODULE_NAME = "update_ozon_prices"
BATCH_SIZE = 950
DELAY = 2


def send_price_updates(price_updates: list) -> list:
    """Отправляет обновления цен на Ozon пакетами."""
    total_batches = (len(price_updates) + BATCH_SIZE - 1) // BATCH_SIZE
    report = []

    for i in range(0, len(price_updates), BATCH_SIZE):
        batch = price_updates[i : i + BATCH_SIZE]
        payload = {"prices": batch}

        response = requests.post(OZON_API_URL, headers=OZON_HEADERS, json=payload, verify=False)

        status = "updated" if response.status_code == 200 else "error"
        if response.status_code == 200:
            write_log(f"✅ Пакет {i // BATCH_SIZE + 1}/{total_batches}: {len(batch)} записей")
        else:
            write_log(f"❌ Пакет {i // BATCH_SIZE + 1}: {response.text[:200]}")

        for item in batch:
            report.append({
                "offer_id": item["offer_id"],
                "old_price": item["old_price"],
                "new_price": item["price"],
                "status": status,
            })

        time.sleep(DELAY)

    return report


def main():
    try:
        client = sheets.get_client()
        sh = client.open_by_key(SPREADSHEET_ID)

        try:
            ws = sh.worksheet("update_ozon_prices")
        except Exception:
            ws = sh.add_worksheet(title="update_ozon_prices", rows=100, cols=10)

        ws.clear()
        time.sleep(0.5)

        # Читаем прайс Ozon
        wsconf = sh.worksheet("generate_ozon_file")
        data = wsconf.get_all_values()
        df = get_config.init_conf_df(pd.DataFrame(data))
        df = df[["Артикул*", "Название товара", "Цена, руб.*"]]

        # Формируем обновления
        price_updates = []
        for _, row in df.iterrows():
            articul = str(row["Артикул*"]).strip()
            try:
                new_price = float(str(row["Цена, руб.*"]).replace(",", "."))
            except ValueError:
                write_log(f"❌ Некорректная цена: {articul}")
                continue

            old_price = round(new_price * 1.11, 2)
            min_price = round(new_price * 0.97, 2)

            price_updates.append({
                "auto_action_enabled": "DISABLED",
                "auto_add_to_ozon_actions_list_enabled": "DISABLED",
                "currency_code": "RUB",
                "min_price": str(round(min_price)),
                "min_price_for_auto_actions_enabled": True,
                "net_price": "",
                "offer_id": articul,
                "old_price": str(round(old_price)),
                "price": str(round(new_price)),
                "price_strategy_enabled": "DISABLED",
                "quant_size": 1,
                "vat": "0",
            })

        # Отправляем
        if price_updates:
            report_rows = send_price_updates(price_updates)
            write_log(f"Обновлено: {len(report_rows)} артикулов")
        else:
            report_rows = []
            write_log("⚠️ Нет данных для обновления")

        # Отчёт в Google Sheets
        report_df = pd.DataFrame(report_rows)
        df_updates = pd.DataFrame(price_updates)
        if not report_df.empty and not df_updates.empty:
            df_final = df_updates.merge(report_df, on="offer_id", how="left")
        else:
            df_final = pd.DataFrame()

        if not df_final.empty:
            upload_df_to_sheet(ws, df_final, sheet_label="update_ozon_prices")

        write_log(f"[{MODULE_NAME}] Завершено")
        finish(MODULE_NAME)

    except Exception as e:
        tb = traceback.format_exc()
        write_log(f"ERROR: {e}")
        write_log(tb)
        write_status("error", str(e), MODULE_NAME)


if __name__ == "__main__":
    main()
