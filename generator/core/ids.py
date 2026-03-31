"""
core/ids.py — Генерация уникальных идентификаторов объявлений.
"""

from datetime import datetime
from typing import List

import pandas as pd

from shared.logger import print_log


def create_id_list(name_csv: str, count: int) -> List[str]:
    """
    Генерирует список ID вида 'name_csv_250328-14-001'.

    Args:
        name_csv: Идентификатор аккаунта.
        count: Количество ID.

    Returns:
        Список строковых ID.
    """
    now = datetime.now()
    prefix = now.strftime("%y%m%d-%H")
    return [f"{name_csv}_{prefix}-{i:03}" for i in range(1, count + 1)]


def create_and_process_id(cl, df: pd.DataFrame) -> pd.DataFrame:
    """
    Генерирует и записывает Id для каждого объявления.

    Формат: {дата}_{артикул}_{порядковый_id}[_gip-{гипотеза}]

    Args:
        cl: ClientParams.
        df: DataFrame прайса.

    Returns:
        DataFrame с заполненным столбцом Id.
    """
    art_gip_list = df["art-gip"].tolist() if "art-gip" in df.columns else [""] * len(df)
    articul_list = df["articul"].tolist()

    date_beg = (
        df["DateBegin"].tolist()
        if "DateBegin" in df.columns
        else [""] * len(df)
    )

    id_list = create_id_list(cl.name_csv, cl.num_ads)

    print_log(f"Генерация ID: {len(df)} строк, {len(id_list)} ID")

    for i, row in df.iterrows():
        art_gip = row.get("art-gip", "") if "art-gip" in row and pd.notna(row.get("art-gip")) else ""
        gip_suffix = f"_gip-{art_gip}" if art_gip else ""

        date_part = str(date_beg[i])[:13] if pd.notna(date_beg[i]) else ""
        sep = "_" if date_part else ""

        if i < cl.num_ads:
            df.at[i, "Id"] = f"{date_part}{sep}{articul_list[i]}_{id_list[i]}{gip_suffix}"
        else:
            df.at[i, "Id"] = f"bag_idx_{i}"

    return df
