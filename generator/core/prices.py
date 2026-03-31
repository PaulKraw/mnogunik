"""
core/prices.py — Работа с прайсом: дублирование строк, распределение по городам.

Функции:
- read_city_distribution: чтение CSV с городами и пропорциями
- duplicate_rows_robust: дублирование строк прайса по городам
- replace_grand_values: замена grand(min,max,step) на случайные числа
- write_city_list_csv: создание CSV-файла городов
"""

import csv
import os
import random
import re
from collections import defaultdict
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from shared.config import ROOT_DIR
from shared.logger import print_log


def read_city_distribution(file_path: str, target_count: int) -> Dict[str, int]:
    """
    Читает CSV с распределением городов и рассчитывает кол-во строк на город.

    Args:
        file_path: Путь к CSV (столбцы: Город, число).
        target_count: Целевое количество строк.

    Returns:
        Словарь {город: количество_строк}.
    """
    city_distribution: Dict[str, int] = defaultdict(int)
    total_ratio = 0

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row["Город"].strip()
            if city:
                ratio = int(row["число"])
                city_distribution[city] += ratio
                total_ratio += ratio

    if total_ratio == 0:
        raise ValueError("Общая доля = 0. Проверьте 'число' в файле городов.")

    for city in city_distribution:
        city_distribution[city] = round(
            (city_distribution[city] / total_ratio) * target_count
        )

    # Коррекция округления
    total_assigned = sum(city_distribution.values())
    diff = target_count - total_assigned
    if diff != 0:
        first_city = next(iter(city_distribution))
        city_distribution[first_city] += diff

    return dict(city_distribution)


def duplicate_rows_robust(
    df: pd.DataFrame,
    target_count: int,
    city_distribution: Dict[str, int],
    city_col: str = "Город",
    all_key: str = "все",
    shuffle: bool = False,
) -> pd.DataFrame:
    """
    Дублирует строки прайса по распределению городов.

    Логика:
    1. countown > 0 → фиксированное дублирование (ровно countown раз).
    2. Город != 'все' без countown → не трогаем.
    3. Город == 'все' без countown → пул для распределения по городам.

    Args:
        df: Исходный прайс.
        target_count: Целевое кол-во строк (информационно).
        city_distribution: Словарь {город: кол-во}.
        city_col: Имя колонки с городом.
        all_key: Значение «все города».
        shuffle: Перемешать результат.

    Returns:
        DataFrame с дублированными строками.
    """
    if city_col not in df.columns:
        raise ValueError(f"Нет колонки '{city_col}' в DataFrame")

    work = df.copy()
    work[city_col] = work[city_col].astype(str).str.strip()
    work["_city_norm"] = work[city_col].str.lower().str.strip()

    all_norm = str(all_key).strip().lower()

    has_countown = "countown" in work.columns
    if has_countown:
        work["_countown_num"] = pd.to_numeric(work["countown"], errors="coerce")
    else:
        work["_countown_num"] = np.nan

    # 1) Фиксированные строки (countown > 0)
    fixed_mask = work["_countown_num"].fillna(0) > 0
    fixed_rows = work[fixed_mask].drop(columns=["_city_norm"])
    fixed_list = []
    if not fixed_rows.empty:
        reps = fixed_rows["_countown_num"].astype(int).tolist()
        repeated = fixed_rows.loc[fixed_rows.index.repeat(reps)].copy()
        fixed_list.append(repeated)

    # 2) Нетронутые (не-'все' без countown)
    flex_mask = ~fixed_mask
    untouched_mask = flex_mask & (work["_city_norm"] != all_norm)
    untouched_rows = work[untouched_mask].drop(columns=["_city_norm"])

    # 3) Пул 'все' для распределения
    pool_mask = flex_mask & (work["_city_norm"] == all_norm)
    pool_rows = work[pool_mask].drop(columns=["_city_norm"])

    # 4) Распределение по городам
    norm_dist = [(str(k), int(v)) for k, v in city_distribution.items() if int(v) > 0]
    dist_clones = []

    if not pool_rows.empty and norm_dist:
        for city_target, need_count in norm_dist:
            if need_count <= 0:
                continue
            reps = (need_count // len(pool_rows)) + 1
            chunk = pd.concat([pool_rows] * reps, ignore_index=True).iloc[:need_count].copy()
            chunk[city_col] = city_target
            dist_clones.append(chunk)

    # 5) Сборка
    parts = []
    if fixed_list:
        parts.append(pd.concat(fixed_list, ignore_index=True))
    if not untouched_rows.empty:
        parts.append(untouched_rows.reset_index(drop=True))
    if dist_clones:
        parts.append(pd.concat(dist_clones, ignore_index=True))

    if parts:
        out = pd.concat(parts, ignore_index=True)
    else:
        out = df.copy().iloc[0:0]

    out = out.reindex(columns=df.columns, fill_value=None)

    if shuffle and len(out) > 1:
        out = out.sample(frac=1, random_state=42).reset_index(drop=True)

    return out


def replace_grand_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Заменяет grand(x,y,z) на случайное число из диапазона [x, y] с шагом z.

    Args:
        df: DataFrame с ячейками, содержащими grand(...).

    Returns:
        DataFrame с заменёнными значениями.
    """
    pattern = re.compile(r"grand\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)")

    def _replace_cell(cell):
        if not isinstance(cell, str):
            return cell

        def _repl(m):
            x, y, z = int(m.group(1)), int(m.group(2)), int(m.group(3))
            values = list(range(x, y + 1, z))
            return str(random.choice(values)) if values else "0"

        return pattern.sub(_repl, cell)

    return df.applymap(_replace_cell)


def write_city_list_csv(
    params,
    root_dir: str,
    shuffle: bool = False,
    encoding: str = "utf-8-sig",
    logger=print_log,
) -> str:
    """
    Создаёт CSV-файл со списком городов на основе распределения.

    Args:
        params: ClientParams с name, k_gorod, name_csv, date_f, num_ads.
        root_dir: Корневая директория проекта.
        shuffle: Перемешать строки.
        encoding: Кодировка файла.
        logger: Функция логирования.

    Returns:
        Путь к созданному CSV.
    """
    city_file = f"{root_dir}/{params.name}/{params.k_gorod}"
    if not os.path.isfile(city_file):
        raise FileNotFoundError(f"Файл городов не найден: {city_file}")

    city_dist = read_city_distribution(city_file, params.num_ads)
    items = list(city_dist.items()) if isinstance(city_dist, dict) else list(city_dist)

    rows = []
    for city, n in items:
        n = int(n)
        if n > 0:
            rows.extend([str(city)] * n)

    if shuffle and rows:
        random.Random(42).shuffle(rows)

    out_dir = f"{root_dir}/{params.name}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = (
        f"{out_dir}/cities_{params.name}_{params.name_csv}"
        f"_{params.date_f}_{params.num_ads}.csv"
    )

    pd.DataFrame({"Город": rows}).to_csv(out_path, index=False, encoding=encoding)

    if logger:
        logger(f"✅ Файл городов: {out_path} ({len(rows)} строк)")

    return out_path
