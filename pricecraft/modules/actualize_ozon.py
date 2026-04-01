#!/usr/bin/env python3
"""
modules/actualize_ozon.py — Актуализация товаров на Ozon.

Сравнивает артикулы в нашей базе с активными на Ozon,
архивирует лишние товары.
"""

import json
import os
import time
import traceback
from datetime import datetime
from typing import Dict, List

import pandas as pd
import requests

from pricecraft.config import sheets, settings
from shared.config import SPREADSHEET_ID
from shared.avito_api import get_ozon_headers
OZON_HEADERS = get_ozon_headers()
from shared.logger import write_log, write_status
from pricecraft.utils.button_status import finish
from pricecraft.modules import get_config

MODULE_NAME = "actualize_ozon"
RUNNERS_DIR = settings.RUNNERS_DIR


def get_active_ozon_articles_with_product_ids() -> Dict[str, int]:
    """
    Получает все активные товары с Ozon.

    Returns:
        Словарь {offer_id: product_id} для активных (не архивных) товаров.
    """
    url = "https://api-seller.ozon.ru/v3/product/list"
    result: Dict[str, int] = {}
    last_id = ""

    write_log("📥 Получение товаров с Ozon...")

    while True:
        payload = {"filter": {"visibility": "ALL"}, "limit": 1000}
        if last_id:
            payload["last_id"] = last_id

        try:
            resp = requests.post(url, headers=OZON_HEADERS, json=payload, verify=False, timeout=30)
            if resp.status_code != 200:
                write_log(f"❌ API ошибка: {resp.status_code}")
                break

            data = resp.json()
            items = data.get("result", {}).get("items", [])
            if not items:
                break

            for item in items:
                offer_id = item.get("offer_id", "")
                product_id = item.get("id") or item.get("product_id")
                is_archived = item.get("is_archived", False)

                if offer_id and product_id and not is_archived:
                    result[offer_id] = int(product_id)

            last_id = data.get("result", {}).get("last_id", "")
            write_log(f"📊 Активных: {len(result)}")

            if not last_id:
                break
            time.sleep(0.5)

        except Exception as e:
            write_log(f"❌ Ошибка: {e}")
            break

    write_log(f"✅ Итого активных: {len(result)}")
    return result


def get_our_articles() -> List[str]:
    """Получает наши артикулы из листа generate_ozon_file."""
    try:
        client = sheets.get_client()
        sh = client.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet("generate_ozon_file")
        data = ws.get_all_values()
        df = get_config.init_conf_df(pd.DataFrame(data))

        for col in ("Артикул*", "Артикул продавца"):
            if col in df.columns:
                articles = df[col].dropna().astype(str).str.strip().tolist()
                write_log(f"📊 Наших артикулов: {len(articles)}")
                return articles

        write_log("⚠️ Столбец артикулов не найден")
        return []
    except Exception as e:
        write_log(f"❌ Ошибка получения артикулов: {e}")
        return []


def archive_by_product_ids(product_ids: List[int]) -> Dict:
    """Архивирует товары на Ozon по product_id."""
    if not product_ids:
        return {"archived": 0, "errors": []}

    url = "https://api-seller.ozon.ru/v1/product/archive"
    archived = 0
    errors = []

    for i in range(0, len(product_ids), 100):
        batch = product_ids[i : i + 100]
        try:
            resp = requests.post(
                url, headers=OZON_HEADERS, json={"product_id": batch}, timeout=30
            )
            if resp.status_code == 200:
                archived += len(batch)
                write_log(f"✅ Архивировано: {len(batch)}")
            else:
                write_log(f"❌ Ошибка: {resp.text[:200]}")
                errors.extend([str(p) for p in batch])
            time.sleep(1)
        except Exception as e:
            write_log(f"❌ {e}")
            errors.extend([str(p) for p in batch])

    return {"archived": archived, "errors": errors}


def save_report(
    our_count: int, ozon_count: int, archived_count: int,
    archived_list: List[str], errors: List[str],
) -> None:
    """Сохраняет отчёт в Google Sheets."""
    try:
        client = sheets.get_client()
        sh = client.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet("ozon_actualize_report")
        except Exception:
            ws = sh.add_worksheet(title="ozon_actualize_report", rows=1000, cols=10)

        ws.clear()

        stats = pd.DataFrame({
            "Дата": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            "Наших": [our_count],
            "На Ozon": [ozon_count],
            "Архивировано": [archived_count],
            "Ошибок": [len(errors)],
        })

        rows = [stats.columns.tolist()] + stats.astype(str).values.tolist()
        ws.update("A1", rows, value_input_option="USER_ENTERED")
        write_log("📊 Отчёт сохранён")
    except Exception as e:
        write_log(f"⚠️ Ошибка отчёта: {e}")


def main():
    try:
        write_log("=" * 60)
        write_log("🔄 ЗАПУСК АКТУАЛИЗАЦИИ OZON")
        write_status("running", "Актуализация...", MODULE_NAME)

        # 1. Наши артикулы
        our = get_our_articles()
        if not our:
            write_status("error", "Нет артикулов", MODULE_NAME)
            return

        # 2. Товары Ozon
        ozon_map = get_active_ozon_articles_with_product_ids()

        # 3. Сравнение
        ozon_ids = set(ozon_map.keys())
        our_ids = set(our)
        to_archive = [ozon_map[oid] for oid in (ozon_ids - our_ids) if oid in ozon_map]

        write_log(f"📊 Ozon: {len(ozon_ids)}, наших: {len(our_ids)}, к архивации: {len(to_archive)}")

        if not to_archive:
            write_log("🎉 Нет лишних товаров")
            save_report(len(our), len(ozon_map), 0, [], [])
            write_status("finished", "Нет товаров для архивации", MODULE_NAME)
            finish(MODULE_NAME)
            return

        # 4. Архивация
        time.sleep(3)
        result = archive_by_product_ids(to_archive)

        # 5. Отчёт
        archived_offers = [o for o, p in ozon_map.items() if p in to_archive]
        save_report(len(our), len(ozon_map), result["archived"], archived_offers, result["errors"])

        write_log(f"✅ Архивировано: {result['archived']}, ошибок: {len(result['errors'])}")
        write_status("finished", f"Архивировано {result['archived']}", MODULE_NAME)
        finish(MODULE_NAME)

    except Exception as e:
        write_log(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        write_log(traceback.format_exc())
        write_status("error", str(e)[:100], MODULE_NAME)


if __name__ == "__main__":
    main()
