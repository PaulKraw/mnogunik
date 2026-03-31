"""
utils/data_fetcher.py — Загрузка данных из Google Sheets (CSV / XLSX).

Вынесено из get_config.py для переиспользования.
"""

import os
from typing import Optional

import openpyxl
import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def download_csv(
    url_batya: str,
    gid: int,
    name_table: str,
    save_dir: str = "csv",
) -> pd.DataFrame:
    """
    Скачивает лист Google Sheets как CSV и возвращает DataFrame.

    Args:
        url_batya: Базовый URL таблицы.
        gid: GID листа.
        name_table: Имя файла для сохранения.
        save_dir: Папка для сохранения.

    Returns:
        DataFrame с данными.
    """
    os.makedirs(save_dir, exist_ok=True)
    filepath = os.path.join(save_dir, name_table)
    url = f"{url_batya}/export?format=csv&gid={gid}"

    response = requests.get(url, verify=False)
    response.raise_for_status()

    with open(filepath, "wb") as f:
        f.write(response.content)

    df = pd.read_csv(filepath)
    df = df.apply(
        lambda col: col.map(
            lambda x: f"{x:.0f}"
            if pd.notna(x) and isinstance(x, float) and x == int(x)
            else x
        )
    )
    return df


def download_xlsx_colored(
    url_batya: str,
    gid: int,
    name_table: str,
    min_row: int,
    min_col: int,
    target_rgb: str,
) -> pd.DataFrame:
    """
    Скачивает лист как XLSX и извлекает данные по цвету ячеек.

    Ячейки с цветом target_rgb → берём значение.
    Остальные числовые ячейки → None.

    Args:
        url_batya: Базовый URL таблицы.
        gid: GID листа.
        name_table: Имя файла.
        min_row: Начальная строка (1-based).
        min_col: Начальный столбец (1-based).
        target_rgb: Цвет для извлечения (hex без альфа, напр. '00FFFF').

    Returns:
        DataFrame с извлечёнными данными.
    """
    url = f"{url_batya}/export?format=xlsx&gid={gid}"
    response = requests.get(url, verify=False)
    response.raise_for_status()

    with open(name_table, "wb") as f:
        f.write(response.content)

    wb = openpyxl.load_workbook(name_table, data_only=True)
    sheet = wb.active

    data = []
    for row in sheet.iter_rows(min_row=min_row, min_col=min_col):
        row_data = []
        for cell in row:
            fill = cell.fill
            color = fill.start_color.rgb

            if isinstance(color, str):
                rgb = color[2:]  # обрезаем альфа
            else:
                rgb = "FFFFFF"

            if rgb == target_rgb:
                row_data.append(cell.value)
            else:
                if isinstance(cell.value, (int, float)) or cell.value == "#REF!":
                    row_data.append(None)
                else:
                    row_data.append(cell.value)

        data.append(row_data)

    return pd.DataFrame(data)
