#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
go.py — Точка входа: оркестратор генерации объявлений.

Читает параметры из Google Sheets, для каждого клиента/аккаунта:
1. Загружает прайс
2. Дублирует строки по городам
3. Обрабатывает тексты, картинки, даты, ID, адреса
4. Удаляет служебные столбцы
5. Сохраняет CSV + XML
6. Генерирует HTML-превью

Запуск: python -m mnogunik.go
"""

import os
import time
from datetime import datetime

import pandas as pd

# ── Конфигурация ──
from shared.config import ROOT_DIR, ROOT_DIR_OUT, ROOT_URL_OUT, IS_LOCAL

# ── Логирование ──
from shared.logger import get_logger, reset_log, print_log

# ── Утилиты ──
from shared.google_sheets import download_csv, make_csv_url
from generator.utils.helpers import format_execution_time

# ── Ядро ──
from generator.core.params_reader import read_params_from_csv
from generator.core.prices import (
    read_city_distribution,
    duplicate_rows_robust,
    replace_grand_values,
    write_city_list_csv,
)
from generator.core.text import create_and_process_text
from generator.core.dates import create_and_process_date
from generator.core.ids import create_and_process_id
from generator.core.addresses import create_and_process_adres
from generator.core.images import create_and_process_img_url, write_progress
from generator.core.export import (
    read_columns_to_delete,
    delete_columns,
    generate_html_from_df,
)

# ── XML ──
from generator.xml.builder import save_avito_xml_to_file
from generator.utils.helpers import path_to_html_link

# ═══════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ
# ═══════════════════════════════════════════

def check_time(start: float) -> None:
    """Логирует время с момента старта."""
    elapsed = time.time() - start
    print_log(format_execution_time(elapsed))


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════

def main() -> None:
    """Основной цикл генерации объявлений."""
    # Инициализация
    # reset_log()
    reset_log(log_file='generator/log.txt')
    logger = get_logger()
    start_time = time.time()

    # ── 1. Загрузка параметров из Google Sheets ──
    params_url = (
        "https://docs.google.com/spreadsheets/d/"
        "1tLVJCMAqYxzHw1SgwT8UcaM4eH6tccMhg8szjBnnkHk/export?format=csv"
    )
    params_file = "cl_rows.csv"
    # временное отключение для доступа сразу из текстового
    download_csv(params_url, params_file)
    # params_file = "C:/code/mnogunik/"+params_file
    params_list = read_params_from_csv(params_file)

    # ── 2. Обработка каждого клиента/аккаунта ──
    for params in params_list:
        print_log(f"═══ Аккаунт: {params.name} — {params.name_csv} ═══")
        write_progress(0)
        check_time(start_time)

        # ── 2.1 Загрузка прайса ──
        output_file_path = (
            f"{ROOT_DIR}/{params.name}/{params.name}_{params.name_csv}"
            f"_{params.date_f}_{params.num_ads}.csv"
        )

        # Удаляем старый файл если есть
        if os.path.exists(output_file_path):
            try:
                os.remove(output_file_path)
                print_log(f"Старый файл удалён: {output_file_path}")
            except Exception as e:
                print_log(f"Ошибка удаления: {e}")

        if params.file_price and params.file_price.startswith(("http://", "https://")):
            # Прайс из Google Sheets
            csv_url = make_csv_url(params.file_price)
            price_path = (
                f"{ROOT_DIR}/{params.name}/var/"
                f"file_price_{params.name}_{params.name_csv}.csv"
            )
            download_csv(csv_url, price_path)
        else:
            # Локальный файл
            price_path = f"{ROOT_DIR}/{params.name}/var/{params.file_price}"

        price_df = pd.read_csv(price_path, dtype=str)

        # ── 2.2 Дублирование строк по городам ──
        if "countown" in price_df.columns:
            city_file = f"{ROOT_DIR}/{params.name}/{params.k_gorod}"
            city_dist = read_city_distribution(str(city_file), params.num_ads)

            cities_csv = write_city_list_csv(params, ROOT_DIR, shuffle=False, logger=print_log)
            print_log(f"CSV городов: {cities_csv}")

            print_log(f"Дублирование строк: {params.name} — {params.name_csv}")
            extended_df = duplicate_rows_robust(price_df, params.num_ads, city_dist)

            rows, cols = extended_df.shape
            print_log(f"Строк: {rows}, Столбцов: {cols}")

            extended_df = replace_grand_values(extended_df)
        else:
            extended_df = price_df

        # ── 2.3 Обработка уникальных текстов (если есть) ──
        if "temp_unik_Description" in extended_df.columns:
            print_log(f"Обработка уникальных текстов: {params.name} — {params.name_csv}")
            # Используем ту же функцию — она проверяет наличие столбца
            from generator.core.text import create_and_process_unik_text
            extended_df = create_and_process_unik_text(params, extended_df, ROOT_DIR)

        # ── 2.4 Обработка текстов Description ──
        if "temp_Description" in extended_df.columns:
            print_log(f"Обработка текстов: {params.name} — {params.name_csv}")
            extended_df = create_and_process_text(params, extended_df, ROOT_DIR)

        check_time(start_time)

        # ── 2.5 Обработка картинок ──
        # k=1 — генерация файлов и ссылок; k=11 — только ссылки
        k = 1
        img_gen = (k == 11)
        if img_gen:
            print_log("Генерация: только текст (без картинок)")
        else:
            print_log("Генерация: файлы и картинки")

        print_log(f"Обработка картинок: {params.name} — {params.name_csv}")
        extended_df = create_and_process_img_url(params, extended_df, ROOT_DIR, img_gen)
        check_time(start_time)

        # ── 2.6 Обработка дат ──
        if "DateBegin" in extended_df.columns:
            print_log(f"Обработка дат: {params.name} — {params.name_csv}")
            extended_df = create_and_process_date(params, extended_df)

        # ── 2.7 Генерация ID ──
        if "countown" in extended_df.columns:
            print_log(f"Обработка ID: {params.name} — {params.name_csv}")
            extended_df = create_and_process_id(params, extended_df)

        check_time(start_time)

        # ── 2.8 Обработка адресов ──
        if "Address" in extended_df.columns:
            print_log(f"Обработка адресов: {params.name} — {params.name_csv}")
            extended_df = create_and_process_adres(params, extended_df)

        # ── 2.9 Удаление служебных столбцов ──
        del_col_path = f"{ROOT_DIR}/{params.name}/var/del_col.txt"
        if os.path.exists(del_col_path):
            cols_to_del = read_columns_to_delete(del_col_path)
            extended_df = delete_columns(extended_df, cols_to_del)

        extended_df["numad"] = range(len(extended_df))

        # ── 2.10 Сохранение CSV ──
        extended_df.to_csv(output_file_path, index=False)
        print_log(f"CSV: {output_file_path}")
        print_log(f" -------- -------- -------- -------- -------- -------- -------- -------- -------- -------- ")
        print_log(f"✅ CSV: {path_to_html_link(output_file_path, 'csv файл  '+params.name_csv)}")
        print_log(f" -------- -------- -------- -------- -------- -------- -------- -------- -------- -------- ")
        
        check_time(start_time)

        # ── 2.11 Сохранение XML ──
        print_log("Генерация XML для Авито...")
        xml_path = save_avito_xml_to_file(extended_df, params, output_file_path)
        print_log(f"XML: {xml_path}")
        check_time(start_time)

        # ── 2.12 HTML-превью ──
        print_log("Создание HTML-превью...")
        html_path = f"{ROOT_DIR_OUT}/{params.name}_{params.name_csv}_output.html"

        print_log(f" -------- -------- -------- -------- -------- -------- -------- -------- -------- -------- ")
        print_log(f"✅ prev: {path_to_html_link(html_path, 'превью для файла  '+params.name_csv)}")
        print_log(f" -------- -------- -------- -------- -------- -------- -------- -------- -------- -------- ")
        
        
        generate_html_from_df(extended_df, html_path, params.name_csv)
        check_time(start_time)

    # ── 3. Итог ──
    total = time.time() - start_time
    print_log(format_execution_time(total))
    print_log(f"Общее время: {total:.2f} секунд")
    print_log(f"Завершено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ═══════════════════════════════════════════
# ЗАПУСК
# ═══════════════════════════════════════════

if __name__ == "__main__":
    main()
