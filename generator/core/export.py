"""
core/export.py — Экспорт данных: HTML-превью, слияние CSV, WordPress.
"""

import os
from typing import Optional

import pandas as pd


def merge_csv_files(
    csv_file1: str, csv_file2: str, output_path: str
) -> None:
    """
    Объединяет два CSV-файла по общим столбцам (outer join).

    Args:
        csv_file1: Путь к первому CSV.
        csv_file2: Путь ко второму CSV.
        output_path: Путь для сохранения результата.
    """
    df1 = pd.read_csv(csv_file1, dtype=str)
    df2 = pd.read_csv(csv_file2, dtype=str)
    common = df1.columns.intersection(df2.columns).tolist()
    merged = pd.merge(df1, df2, on=common, how="outer")
    merged.to_csv(output_path, index=False)


def clean_merged_data(file_path: str) -> None:
    """
    Очищает объединённый файл: удаляет AvitoStatus, пустые колонки.

    Args:
        file_path: Путь к CSV-файлу.
    """
    df = pd.read_csv(file_path, dtype=str)

    if "AvitoStatus" in df.columns:
        df.drop(columns=["AvitoStatus"], inplace=True)

    df.dropna(axis=1, how="all", inplace=True)

    if "Availability" in df.columns:
        df["Availability"] = df["Availability"].fillna("В наличии")

    df.to_csv(file_path, index=False)


def generate_html_from_df(df: pd.DataFrame, output_path: str) -> None:
    """
    Генерирует HTML-страницу для визуальной проверки объявлений.

    Выбирает до 100 случайных строк и отображает их с картинками.

    Args:
        df: DataFrame прайса.
        output_path: Путь для сохранения HTML.
    """
    sample = df.sample(n=min(100, len(df)), random_state=1) if len(df) > 100 else df

    html = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Preview</title>
<style>
body{font-family:Arial;margin:20px;display:flex;flex-wrap:wrap;background:#303030}
.row{flex:1 1 48%;min-width:400px;background:#bebebe;margin:.5em;display:flex;flex-wrap:wrap;gap:8px}
.row-min{display:flex;flex-wrap:wrap;margin:5px;order:3}
.row-min.title .column-value{font-size:150%;font-weight:700}
.row-min.price .column-value{font-size:125%;font-weight:700}
.column-title{font-weight:bold;background:#a1a1a1;padding:.3em;min-width:50px}
.column-value{margin-left:10px;padding:.3em}
.column-value img{max-width:500px;margin:5px}
</style></head><body>
"""
    for _, row in sample.iterrows():
        html += '<div class="row">\n'
        for col in df.columns:
            val = row[col]
            if pd.isna(val) or val == "":
                continue

            cls = ""
            if col == "Title":
                cls = " title"
            elif col == "Price":
                cls = " price"

            html += f'<div class="row-min{cls}">'
            html += f'<div class="column-title">{col}:</div>'

            if isinstance(val, str) and "http" in val and ".jpg" in val:
                imgs = [l for l in val.split(" | ") if l.lower().endswith(".jpg")]
                tags = "".join(
                    f'<img src="{u}" style="max-width:500px;height:auto">'
                    for u in imgs
                )
                html += f'<div class="column-value">{tags}</div>'
            else:
                html += f'<div class="column-value">{val}</div>'

            html += "</div>\n"
        html += "</div>\n"

    html += "</body></html>"

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


def read_columns_to_delete(file_path: str) -> list:
    """Читает список столбцов для удаления из текстового файла."""
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def delete_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Удаляет указанные столбцы из DataFrame."""
    return df.drop(columns=[c for c in columns if c in df.columns])
