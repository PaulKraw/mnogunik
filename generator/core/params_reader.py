"""
core/params_reader.py — Чтение параметров клиентов из CSV (Google Sheets).

Функция read_params_from_csv вынесена из textfun.py.
"""

import ast
import csv
import json
import os
from typing import List

from shared.config import ROOT_DIR
from generator.klass import ClientParams


def read_params_from_csv(file_path: str) -> List[ClientParams]:
    """
    Читает CSV-файл с параметрами клиентов и возвращает список ClientParams.

    Файл обычно загружается из Google Sheets (cl_rows.csv).
    Обрабатываются только строки с run=1.

    Args:
        file_path: Путь к CSV-файлу.

    Returns:
        Список объектов ClientParams для обработки.
    """
    params_list: List[ClientParams] = []

    print(file_path)
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("run") != "1":
                continue

            # Загрузка параметров изображений
            json_path = f"{ROOT_DIR}/{row['name']}/var/img/img.json"
            with open(json_path, "r", encoding="utf-8") as jf:
                imgparam = json.load(jf)

            # Парсинг периодов
            periods_raw = row.get("periods", "")
            periods = []
            if periods_raw:
                try:
                    periods = ast.literal_eval(periods_raw)
                except (ValueError, SyntaxError):
                    periods = []

            params = ClientParams(
                name=row["name"],
                name_csv=row["name_csv"],
                cat_wp=row.get("cat_wp"),
                k_gorod=f"var/adres/{row['k_gorod']}",
                num_ads=int(row["num_ads"]),
                date_f=row["date_f"],
                num_days=1,
                end_date=30,
                file_price=row["file_price"],
                periods=periods or "",
                imgparam=imgparam,
                address_to_append=row.get("address_to_append", ""),
                info_dict={
                    "CompanyName": row.get("CompanyName", ""),
                    "EMail": row.get("EMail", ""),
                    "ContactMethod": row.get("ContactMethod", ""),
                    "ContactPhone": row.get("ContactPhone", ""),
                    "ManagerName": row.get("ManagerName", ""),
                },
            )
            params_list.append(params)

    return params_list
