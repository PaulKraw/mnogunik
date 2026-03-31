"""
stats/statfun.py — Обработка статистики: фильтрация, объединение, API-запросы.

Функции:
- kick_nulstat: фильтрация объявлений без статистики
- process_ads_folder: ревизия папки с данными
- safe_get/safe_post: HTTP-запросы с retry
"""

import datetime
import json
import math
import os
import shutil
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from shared.config import ROOT_DIR
from shared.logger import print_log


def kick_nulstat(st_par: Dict[str, Any], param=None) -> None:
    """
    Фильтрует объявления с нулевой статистикой и сохраняет результат.

    Args:
        st_par: Словарь параметров статистики.
        param: ClientParams (опционально).
    """
    print_log("Загрузка данных из файлов статистики")

    try:
        stat_data = pd.read_excel(st_par["file_stat"], dtype=str)
    except Exception as e:
        print_log(f"Ошибка чтения {st_par['file_stat']}: {e}")
        return

    ads_data = pd.read_excel(st_par["file_ads"], dtype=str)

    if param:
        ads_dir = f"{ROOT_DIR}/{param.name}/stat/all_ads_{param.name_csv}"
        os.makedirs(ads_dir, exist_ok=True)
        ads_data.to_csv(f"{ads_dir}/all_ads.csv", index=False)

    # Объединение
    merged = pd.merge(
        ads_data, stat_data,
        left_on=st_par["name_idads"],
        right_on=st_par["name_idstat"],
        how="inner",
    )

    if param:
        merged.to_csv(
            f"{ROOT_DIR}/{param.name}/stat/merged_data_{param.name_csv}.csv",
            index=False,
        )

    print_log("Фильтрация по статистике")
    filtered = merged[
        (merged[st_par["name_pros"]].astype(int) > int(st_par["pros"]))
        | (merged[st_par["name_kont"]].astype(int) > int(st_par["kont"]))
        | (merged[st_par["name_izbr"]].astype(int) > int(st_par["izbr"]))
    ]

    if param:
        filtered.to_csv(
            f"{ROOT_DIR}/{param.name}/stat/filtered_data_{param.name_csv}.csv",
            index=False,
        )

    filtered_ads = filtered[ads_data.columns]

    if param:
        save_path = f"{ROOT_DIR}/{param.name}/stat/{st_par['filtered_ads']}"
    else:
        save_path = f"xls/{st_par['filtered_ads']}"

    filtered_ads.to_csv(save_path, index=False)
    print_log(f"Сохранено: {save_path}")


def process_ads_folder(root_dir: str, params) -> None:
    """
    Ревизия папки с объявлениями: объединение CSV-файлов в all_ads.csv.

    Args:
        root_dir: Корневая директория.
        params: ClientParams.
    """
    folder = f"{root_dir}/{params.name}/stat/ads_{params.name_csv}/"
    arh = os.path.join(folder, "arh")
    os.makedirs(arh, exist_ok=True)

    csv_files = [f for f in os.listdir(folder) if f.endswith(".csv") and f != "arh"]

    if len(csv_files) <= 1:
        print_log("Нет новых файлов для обработки.")
        return

    # Очистка архива
    for old in os.listdir(arh):
        os.remove(os.path.join(arh, old))

    # Перемещение в архив
    for f in csv_files:
        shutil.move(os.path.join(folder, f), os.path.join(arh, f))

    # Объединение
    dfs = [pd.read_csv(os.path.join(arh, f), dtype=str) for f in csv_files]
    merged = pd.concat(dfs, ignore_index=True)

    # Порядок столбцов
    front = ["Id", "AvitoId"]
    rest = [c for c in merged.columns if c not in front]
    merged = merged[front + rest]
    merged["AvitoId"] = merged["AvitoId"].fillna("").astype(str)

    out_path = os.path.join(folder, "all_ads.csv")
    merged.to_csv(out_path, index=False)
    print_log(f"Файлы объединены: {out_path}")


# ═══════════════════════════════════════════
# HTTP с RETRY
# ═══════════════════════════════════════════

def safe_get(
    url: str,
    headers: Dict[str, str],
    retries: int = 3,
    timeout: int = 10,
) -> Optional[requests.Response]:
    """GET-запрос с повторами при сетевых ошибках."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.ConnectionError as e:
            print_log(f"[{attempt + 1}/{retries}] Ошибка соединения: {e}")
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            print_log(f"Ошибка GET: {e}")
            break
    return None


def safe_post(
    url: str,
    headers: Dict[str, str],
    payload: Dict,
    retries: int = 3,
    timeout: int = 10,
) -> Optional[requests.Response]:
    """POST-запрос с повторами при сетевых ошибках."""
    for attempt in range(retries):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.ConnectionError as e:
            print_log(f"[{attempt + 1}/{retries}] Соединение сброшено: {e}")
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            print_log(f"Ошибка POST: {e}")
            break
    return None
