"""
core/dates.py — Генерация дат для объявлений и коррекция часовых поясов.
"""

import csv
import random
from datetime import datetime, time, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dateutil import parser, tz

from shared.logger import print_log


def create_date_list(
    start_date_str: str,
    num_days: int,
    num_ads: int,
    periods: List[Tuple[int, int, int]],
    shuffle: bool = True,
) -> List[str]:
    """
    Генерирует список дат публикации в формате ISO 8601.

    Args:
        start_date_str: Дата начала (YYYY-MM-DD).
        num_days: Количество дней.
        num_ads: Целевое количество дат.
        periods: Список (кол-во_в_день, час_от, час_до).
        shuffle: Перемешать результат.

    Returns:
        Список дат в формате '2025-01-18T09:30:45+03:00'.
    """
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    dates: List[str] = []

    for day in range(num_days):
        for num_per_day, start_hour, end_hour in periods:
            for _ in range(num_per_day):
                rand_time = time(
                    random.randint(start_hour, end_hour),
                    random.randint(0, 59),
                    random.randint(0, 59),
                )
                dt = datetime.combine(start + timedelta(days=day), rand_time)
                dates.append(dt.strftime("%Y-%m-%dT%H:%M:%S+03:00"))

    # Добить пустыми до num_ads
    if len(dates) < num_ads:
        dates.extend([""] * (num_ads - len(dates)))

    if shuffle:
        random.shuffle(dates)

    return dates


def read_city_timezone_file(filename: str) -> Dict[str, int]:
    """Читает CSV с часовыми поясами городов (город, час)."""
    result: Dict[str, int] = {}
    with open(filename, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            result[row["город"]] = int(row["час"])
    return result


def check_timezone(
    dates: List[str],
    cities: List[str],
    city_tz_file: str = "vars/city_gtm.csv",
) -> List[str]:
    """
    Корректирует часовые пояса дат по городу.

    Args:
        dates: Список дат ISO 8601.
        cities: Список городов (параллельный массив).
        city_tz_file: Путь к CSV с часовыми поясами.

    Returns:
        Список дат с исправленными таймзонами.
    """
    city_tz = read_city_timezone_file(city_tz_file)
    corrected: List[str] = []

    for city, date_str in zip(cities, dates):
        try:
            dt = parser.parse(date_str)
            target_tz = city_tz.get(city)
            if target_tz is not None:
                current_tz = dt.tzinfo.utcoffset(dt).total_seconds() / 3600
                if current_tz != target_tz:
                    new_dt = dt.astimezone(tz.tzoffset(None, target_tz * 3600))
                    corrected.append(new_dt.isoformat())
                else:
                    corrected.append(date_str)
            else:
                corrected.append(date_str)
        except Exception:
            corrected.append(date_str)

    return corrected


def create_and_process_date(cl, df: pd.DataFrame) -> pd.DataFrame:
    """
    Генерирует даты и записывает в столбец DateBegin.

    Args:
        cl: ClientParams.
        df: DataFrame прайса.

    Returns:
        DataFrame с заполненным DateBegin.
    """
    if not cl.periods:
        print_log("Периоды не заданы — DateBegin = None")
        df["DateBegin"] = None
        return df

    cities = df["Город"].tolist()

    dates = create_date_list(
        cl.date_f, cl.num_days, cl.num_ads, cl.periods, shuffle=True
    )

    corr = check_timezone(dates, cities)

    for i in range(len(df)):
        df.at[i, "DateBegin"] = corr[i] if i < len(corr) else None

    return df
