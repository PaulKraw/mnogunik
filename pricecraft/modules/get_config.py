"""
modules/get_config.py — Ядро бизнес-логики pricecraft.

Функции:
- init_df / init_conf_df: приведение таблиц к рабочему виду
- getHashTable: создание пар процессор×видеокарта с хешами
- create_table: сборка конфигураций с ценами (Авито)
- create_ozon_table / create_wb_table: прайсы для маркетплейсов
- apply_marketplace_mapping: маппинг параметров по площадкам
- getname_sbopk: генерация названий конфигураций
"""

import hashlib
import math
import os
import random
from typing import Dict, List, Optional

import pandas as pd

from pricecraft.config.settings import PROJECT_ROOT
from pricecraft.utils.data_fetcher import download_csv, download_xlsx_colored
from pricecraft.utils.button_status import finish
from shared.logger import write_log

# Реэкспорт для обратной совместимости (модули импортируют get_config.xxx)
down_respons = download_csv
down_respons_main = download_xlsx_colored


# ═══════════════════════════════════════════
# УТИЛИТЫ
# ═══════════════════════════════════════════

def get_fixed_hash(input_string: str, length: int = 32, prefix: str = "oz_") -> str:
    """Генерирует MD5-хеш фиксированной длины с префиксом."""
    if len(input_string) > 1:
        h = hashlib.md5(input_string.encode()).hexdigest()
        return f"{prefix}{h[:length]}"
    return input_string.replace(" ", "_")


def get_df(name_table: str) -> pd.DataFrame:
    """Читает CSV и форматирует целые числа (убирает .0)."""
    df = pd.read_csv(name_table)
    df = df.apply(
        lambda col: col.map(
            lambda x: f"{x:.0f}"
            if pd.notna(x) and isinstance(x, float) and x == int(x)
            else x
        )
    )
    return df


# ═══════════════════════════════════════════
# ИНИЦИАЛИЗАЦИЯ ТАБЛИЦ
# ═══════════════════════════════════════════

def init_df(df: pd.DataFrame) -> pd.DataFrame:
    """Приводит «шахматку» процессор×видеокарта к рабочему виду."""
    df = df.dropna(how="all").dropna(axis=1, how="all")
    df = df.drop(1, axis=0).drop(df.columns[[1, 2]], axis=1).reset_index(drop=True)
    df.columns = range(df.shape[1])
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    return df


def init_conf_df(df: pd.DataFrame) -> pd.DataFrame:
    """Приводит лист configurations к рабочему виду."""
    df = df.dropna(how="all").dropna(axis=1, how="all")
    df.columns = range(df.shape[1])
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    return df


# ═══════════════════════════════════════════
# ХЕШ-ТАБЛИЦА КОНФИГУРАЦИЙ
# ═══════════════════════════════════════════

def getHashTable(df: pd.DataFrame) -> pd.DataFrame:
    """
    Создаёт таблицу пар процессор×видеокарта из «шахматки».

    Returns:
        DataFrame с колонками: conf, Процессор_ar, Видеокарта_ar, Сумма.
    """
    data = []
    for i in range(1, len(df)):
        for j in range(1, len(df.columns)):
            value = df.iloc[i, j]
            if pd.notna(value):
                strok = df.iloc[i, 0]
                stolb = df.columns[j]
                h = get_fixed_hash(f"{strok}_{stolb}", length=8)
                data.append({
                    "conf": h,
                    "Процессор_ar": strok,
                    "Видеокарта_ar": stolb,
                    "Сумма": value,
                })
    return pd.DataFrame(data)


# ═══════════════════════════════════════════
# РАБОТА С КОМПЛЕКТУЮЩИМИ
# ═══════════════════════════════════════════

def add_column_to_dataframe(
    df_orig: pd.DataFrame,
    df_merg_path: str,
    var_kom: str,
    var_kom_obsh: str,
) -> pd.DataFrame:
    """Добавляет вариации комплектующего через cross join."""
    df_merg = pd.read_csv(df_merg_path)
    df_orig[f"{var_kom}_ar"] = ""

    parts = []
    for _, row in df_merg.iterrows():
        chunk = df_orig.copy()
        chunk[f"{var_kom}_ar"] = row[var_kom_obsh]
        parts.append(chunk)

    return pd.concat(parts, ignore_index=True)


def merge_oz_param(df_orig: pd.DataFrame, name_table: str) -> pd.DataFrame:
    """Присоединяет параметры комплектующего из unique_types/."""
    df_merg = get_df(f"unique_types/{name_table}.csv")
    df_orig[f"{name_table}_ar"] = df_orig[f"{name_table}_ar"].astype(str)
    df_merg["Имя в базе"] = df_merg["Имя в базе"].astype(str)

    return df_orig.merge(
        df_merg,
        left_on=f"{name_table}_ar",
        right_on="Имя в базе",
        how="left",
        suffixes=("", f"_{name_table}"),
    )


def merge_prices(df_orig: pd.DataFrame, name_table: str) -> pd.DataFrame:
    """Присоединяет только цену комплектующего."""
    df_merg = get_df(f"unique_types/{name_table}.csv")
    cols = ["Имя в базе"] + [c for c in df_merg.columns if c == "цена"]
    df_merg = df_merg[cols]

    if "цена" in df_merg.columns:
        df_merg = df_merg.rename(columns={"цена": f"цена_{name_table}"})

    result = df_orig.merge(
        df_merg,
        left_on=f"{name_table}_ar",
        right_on="Имя в базе",
        how="left",
    )
    if "Имя в базе" in result.columns:
        result = result.drop(columns=["Имя в базе"])
    return result


# ═══════════════════════════════════════════
# ГЕНЕРАЦИЯ НАЗВАНИЙ
# ═══════════════════════════════════════════

def getname_sbopk(row) -> str:
    """Генерирует случайное название конфигурации ПК."""
    try:
        types = ["Игровой компьютер", "Игровой ПК"]
        cpu_variants = [
            f"{row['Число ядер процессора']}х{row['Частота процессора, ГГц']} ГГц",
            "",
        ]
        t = random.choice(types)
        c = random.choice(cpu_variants)

        n1 = (
            f"{t} ULTRAFPS ({row['Видеокарта*']}, {row['Процессор*']} {c}, "
            f"{row['Оперативная память*']}, {row['Диск ГБ']} ГБ, {row['Кейс название']})"
        )
        n2 = (
            f"{t} {row['Кейс название']} ULTRAFPS ({row['Видеокарта*']}, "
            f"{row['Процессор*']} {c}, {row['Оперативная память*']}, {row['Диск ГБ']} ГБ)"
        )
        return random.choice([n1, n2])
    except KeyError:
        return "Игровой компьютер"


# ═══════════════════════════════════════════
# МАППИНГ ПАРАМЕТРОВ
# ═══════════════════════════════════════════

def apply_marketplace_mapping(
    df_out: pd.DataFrame,
    df_mapping: pd.DataFrame,
    marketplace: str,
) -> pd.DataFrame:
    """
    Применяет маппинг параметров для маркетплейса.

    Args:
        df_out: Целевой DataFrame.
        df_mapping: Таблица маппинга (source_param, source_value → target_param, target_value).
        marketplace: Имя площадки (ozon, wb, ozon_cont, wb_cont).

    Returns:
        DataFrame с подставленными параметрами.
    """
    df_mp = df_mapping[df_mapping["marketplace"] == marketplace]

    for col in df_mp["target_param_name"].unique():
        if col not in df_out.columns:
            df_out[col] = ""

    for _, row in df_mp.iterrows():
        src_col = row["source_param_name"]
        src_val = row["source_value"]
        tgt_col = row["target_param_name"]
        tgt_val = row["target_value"]

        if pd.isna(src_val) or src_col not in df_out.columns:
            continue

        mask = df_out[src_col] == src_val
        df_out.loc[mask, tgt_col] = tgt_val

    return df_out


# ═══════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ: порядок столбцов, удаление
# ═══════════════════════════════════════════

def _read_column_list(filename: str) -> List[str]:
    """Читает список столбцов из txt-файла."""
    path = os.path.join(PROJECT_ROOT, "txt", filename)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def _apply_column_order(
    df: pd.DataFrame, order_file: str, delete_file: Optional[str] = None
) -> pd.DataFrame:
    """Удаляет столбцы и переупорядочивает по txt-файлам."""
    if delete_file:
        cols_del = _read_column_list(delete_file)
        df = df.drop(cols_del, axis=1, errors="ignore")

    col_order = _read_column_list(order_file)
    existing = [c for c in col_order if c in df.columns]
    remaining = [c for c in df.columns if c not in col_order]
    return df[existing + remaining]


# ═══════════════════════════════════════════
# СОЗДАНИЕ ТАБЛИЦ
# ═══════════════════════════════════════════

def _save_unique_types(df_price_kompl: pd.DataFrame) -> List[str]:
    """Сохраняет CSV-файлы по типам комплектующих, возвращает список типов."""
    unique_types = [str(x) for x in df_price_kompl["Тип компл"].unique()]
    os.makedirs("unique_types", exist_ok=True)

    for comp_type in unique_types:
        if comp_type in ("прочее", "сборка"):
            continue
        filtered = df_price_kompl[df_price_kompl["Тип компл"] == comp_type].copy()
        filtered.replace("", pd.NA, inplace=True)
        filtered = filtered.dropna(axis=1, how="all")
        filtered = filtered.drop(["Тип компл", "Цена из прайса"], axis=1, errors="ignore")

        path = f"unique_types/{comp_type}.csv"
        filtered.to_csv(path, index=False, encoding="utf-8")

    return unique_types


def create_table(
    df_out: pd.DataFrame,
    df_vk_bp: pd.DataFrame,
    df_price_kompl: pd.DataFrame,
    values: Dict,
) -> pd.DataFrame:
    """
    Собирает финальную таблицу конфигураций с ценами (для Авито).

    Args:
        df_out: Хеш-таблица процессор×видеокарта.
        df_vk_bp: Таблица совместимости видеокарта→БП.
        df_price_kompl: Таблица комплектующих с ценами.
        values: Коэффициенты из Google Sheets.

    Returns:
        Финальный DataFrame с ценами и артикулами.
    """
    # БП по видеокарте
    df_out = df_out.merge(
        df_vk_bp[["Видеокарта_ar", "Питание_ar"]],
        on="Видеокарта_ar",
        how="left",
    )

    df_price_kompl = df_price_kompl.dropna(subset=["ОЗОН. Вариации."])
    unique_types = _save_unique_types(df_price_kompl)

    # Вариации (cross join для каждого типа)
    var_types = df_price_kompl[
        pd.to_numeric(df_price_kompl["ОЗОН. Вариации."], errors="coerce") > 0
    ]["Тип компл"].drop_duplicates().tolist()

    for var_kom in var_types:
        if var_kom in ("прочее", "сборка"):
            continue
        df_out = add_column_to_dataframe(
            df_out, f"unique_types/{var_kom}.csv", var_kom, "Имя в базе"
        )

    # Цены
    for name in unique_types:
        df_out = merge_prices(df_out, name)

    # Совместимость процессор↔память
    compatible = set(zip(df_vk_bp["Видеокарта_ar"], df_vk_bp["Питание_ar"]))
    mask = df_out.apply(
        lambda r: (r["Процессор_ar"], r["Память_ar"]) in compatible, axis=1
    )
    df_out = df_out[mask].reset_index(drop=True)

    # Артикулы
    df_out["full_art_hash"] = df_out.apply(
        lambda r: get_fixed_hash(
            f'{r["Процессор_ar"]}_{r["Видеокарта_ar"]}_{r["Питание_ar"]}'
            f'_{r["Память_ar"]}_{r["Диск_ar"]}_{r["Кейс_ar"]}',
            32,
        ),
        axis=1,
    )
    df_out["full_art"] = df_out.apply(
        lambda r: (
            f'{r["Процессор_ar"]}_{r["Видеокарта_ar"]}_{r["Питание_ar"]}'
            f'_{r["Память_ar"]}_{r["Диск_ar"]}_{r["Кейс_ar"]}'
        ),
        axis=1,
    )

    # Цены
    for i, row in df_out.iterrows():
        price = (
            int(values["sborka"])
            + int(values["proch"])
            + int(row.get("цена_Память", 0))
            + int(row.get("цена_Диск", 0))
            + int(row.get("цена_Кейс", 0))
            + int(row.get("цена_Процессор", 0))
            + int(row.get("цена_Видеокарта", 0))
            + int(row.get("цена_Питание", 0))
        )
        df_out.loc[i, "Сумма итог"] = str(price)

    df_out = df_out.rename(columns={"full_art_hash": "articul"})

    # Столбцы по умолчанию
    defaults = {
        "Категория": "игровые пк",
        "Description": "",
        "ImageUrls": "",
        "temp_Description": "text_ozon.txt",
        "first_img": "img_first_pk.json",
    }
    for col, val in defaults.items():
        df_out[col] = val

    return _apply_column_order(df_out, "columns_order.txt", "col_delete.txt")


def create_ozon_table(
    df_out: pd.DataFrame,
    df_price_kompl: pd.DataFrame,
    values: Dict,
    df_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Создаёт прайс для Ozon с маппингом параметров и ценами."""
    _save_unique_types(df_price_kompl)

    df_out = apply_marketplace_mapping(df_out, df_mapping, "ozon")

    ozon_mult = float(str(values.get("ozon", 0)).replace(",", "."))

    for i, row in df_out.iterrows():
        base = float(str(row["Сумма итог"]).replace(",", "."))
        new_price = int(math.ceil(base * ozon_mult * 100) / 100)
        old_price = int(math.ceil(new_price * ozon_mult * 100) / 100)

        df_out.loc[i, "Цена, руб.*"] = str(new_price)
        df_out.loc[i, "Цена до скидки, руб."] = str(old_price)
        df_out.loc[i, "Title"] = getname_sbopk(row)

    df_out = df_out.rename(columns={
        "articul": "Артикул*",
        "Title": "Название товара",
        "Кейс название": "Название модели (для объединения в одну карточку)*",
    })

    return _apply_column_order(df_out, "columns_order_ozon.txt", "col_delete_ozon.txt")


def create_wb_table(
    df_out: pd.DataFrame,
    df_price_kompl: pd.DataFrame,
    values: Dict,
    df_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Создаёт прайс для Wildberries."""
    unique_types = _save_unique_types(df_price_kompl)

    for name in unique_types:
        df_out = merge_oz_param(df_out, name)

    wb_mult = float(str(values.get("wb", 0)).replace(",", "."))

    for i, row in df_out.iterrows():
        base = float(str(row["Сумма итог"]).replace(",", "."))
        new_price = int(math.ceil(base * wb_mult * 100) / 100)
        old_price = int(math.ceil(new_price * wb_mult * 100) / 100)

        df_out.loc[i, "Цена"] = str(new_price)
        df_out.loc[i, "Цена до скидки, руб."] = str(old_price)
        df_out.loc[i, "Title"] = getname_sbopk(row)

    df_out = df_out.rename(columns={
        "articul": "Артикул продавца",
        "Title": "Наименование",
        "Кейс название": "Модель",
    })

    df_out = apply_marketplace_mapping(df_out, df_mapping, "wb")

    return _apply_column_order(df_out, "columns_order_wb.txt", "col_delete_ozon.txt")
