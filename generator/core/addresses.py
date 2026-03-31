"""
core/addresses.py — Обработка адресов объявлений.
"""

import csv
import os
import random
from collections import defaultdict
from typing import Dict, List

import pandas as pd

from shared.config import ROOT_DIR


def read_addresses_with_full_address(filepath: str) -> Dict[str, List[str]]:
    """
    Читает CSV с городами и полными адресами.

    Args:
        filepath: Путь к CSV (столбцы: город, полный_адрес).

    Returns:
        Словарь {город: [адрес1, адрес2, ...]}.
    """
    result: Dict[str, List[str]] = {}
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        next(reader)  # заголовки
        for row in reader:
            city, full_address = row[0], row[1]
            result.setdefault(city, []).append(full_address)
    return result


def create_and_process_adres(cl, df: pd.DataFrame) -> pd.DataFrame:
    """
    Заполняет столбец Address случайными адресами из файла городов.

    Args:
        cl: ClientParams.
        df: DataFrame прайса.

    Returns:
        DataFrame с заполненным Address.
    """
    adres_file = f"{ROOT_DIR}/{cl.name}/var/adres/adres_gorod.csv"
    if not os.path.exists(adres_file):
        adres_file = "vars/adres_gorod.csv"

    addresses = read_addresses_with_full_address(adres_file)
    cities = df["Город"].tolist()

    for index, row in df.iterrows():
        # Если адрес уже заполнен (длина > 2) — пропускаем
        if pd.notna(row.get("Address")):
            addr = str(row["Address"]).strip()
            if len(addr) > 2:
                continue

        city = cities[index]
        if city in addresses:
            df.at[index, "Address"] = random.choice(addresses[city])
        else:
            df.at[index, "Address"] = city

    return df
