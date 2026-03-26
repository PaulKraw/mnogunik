# -*- coding: utf-8 -*-
import sys
# sys.path.append(r'D:\code\mnogunik')
import csv
import pandas as pd
from PIL import Image, ImageDraw, ImageFilter, ImageEnhance, ImageFont
import random
import itertools
import os
import string
import datetime
from collections import defaultdict
import re
import requests

import datetime
import time

import imgunik as img
import avito_xml_builder as txml
import textfun as txt
import statfun as stt
from klass import ClientParams

from config import ROOT_DIR_OUT, ROOT_DIR, ROOT_URL_OUT, nout

with open('log.txt', 'r+') as f:
    f.truncate(0) 

def print_log(msg):
    with open("log.txt", "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    if nout:
        print(msg)


def format_execution_time(execution_time):
    # Рассчитываем минуты и секунды
    minutes = int(execution_time // 60)
    seconds = execution_time % 60
    
    # Функция для определения правильного окончания слова "минута"
    def get_minutes_word(minutes):
        if 11 <= minutes % 100 <= 19:
            return "минут"
        elif minutes % 10 == 1:
            return "минута"
        elif 2 <= minutes % 10 <= 4:
            return "минуты"
        else:
            return "минут"

    # Получаем правильное окончание
    minutes_word = get_minutes_word(minutes)

    return f"Время выполнения программы: {minutes} {minutes_word} ({seconds:.2f} секунд)"
def check_time(start_time):
    end_time = time.time()
    execution_time = end_time - start_time
    toprint = format_execution_time(execution_time)
    print_log(toprint)

    # (toprint)


start_time = time.time()

# ROOT_DIR = 'C:/proj/'

# URL для экспорта Google Sheets в CSV
url = 'https://docs.google.com/spreadsheets/d/1tLVJCMAqYxzHw1SgwT8UcaM4eH6tccMhg8szjBnnkHk/export?format=csv'

# Локальный путь для сохранения файла
file_path = 'cl_rows.csv'

# Загрузка CSV-файла
response = requests.get(url)
response.raise_for_status()  # Проверка успешности запроса

# Сохранение файла на диск
with open(file_path, 'wb') as file:
    file.write(response.content)


try:
    response = requests.get(url, timeout=10)  # Таймаут на случай зависания запроса
    response.raise_for_status()  # Проверяем успешность запроса
    
    # Проверяем, не пустой ли файл
    if not response.content:
        print_log("Ошибка: Файл пустой!")
        # log(msg)
        exit(1)  # Завершаем выполнение с кодом ошибки

    # Сохранение файла
    with open(file_path, "wb") as file:
        file.write(response.content)

    print_log(f"✅ Файл успешно загружен и сохранён: {file_path}")


except (requests.exceptions.RequestException, ValueError) as e:
    print_log(f"Ошибка загрузки: {e}")
    exit(1)  # Выход с кодом ошибки

    # Если файл уже существует, спрашиваем, продолжать ли с локальной версией
    if os.path.exists(file_path):
        user_input = input("Использовать локальный файл? (y/n): ").strip().lower()
        if user_input != "y":
            print_log("⛔ Операция отменена пользователем.")
            exit(1)
        else:
            print_log(f"📂 Используется локальный файл: {file_path}")
    else:
        print_log("❌ Локальный файл отсутствует. Невозможно продолжить.")
        exit(1)


params_list = txt.read_params_from_csv(file_path)

for params in params_list:
    print_log(f"аккаунт : {params.name} - {params.name_csv}")
    txt.write_progress(0)
    check_time(start_time)

    if params.file_price and params.file_price.startswith(("http://", "https://")):
        
        output_file_path = f"{ROOT_DIR}/{params.name}/{params.name}_{params.name_csv}_{params.date_f}_{params.num_ads}.csv"

        # Проверяем существует ли файл и удаляем его
        if os.path.exists(output_file_path):
            try:
                os.remove(output_file_path)
                print(f"Старый файл удален: {output_file_path}")
            except Exception as e:
                print(f"Ошибка при удалении файла {output_file_path}: {e}")
        else:
            print(f"Файл не существует, создаем новый: {output_file_path}")     

        url = txt.make_csv_url_simple(params.file_price)

        # https://docs.google.com/spreadsheets/d/<SPREADSHEET_ID>/export?format=csv&gid=<SHEET_GID>

        # params.file_price
        # https://docs.google.com/spreadsheets/d/1nhQrt2QultdQ_ET1_FZ0G8SRxc6qOtqRKRfBMKxXVWc/edit?gid=290977907#gid=290977907
        #     https://docs.google.com/spreadsheets/d/1nhQrt2QultdQ_ET1_FZ0G8SRxc6qOtqRKRfBMKxXVWc/edit?usp=sharing
                
        # # URL для экспорта Google Sheets в CSV
        # url = 'https://docs.google.com/spreadsheets/d/1tLVJCMAqYxzHw1SgwT8UcaM4eH6tccMhg8szjBnnkHk/export?format=csv'

        # Локальный путь для сохранения файла
        # file_path = 'cl_rows.csv'

        file_path = f"{ROOT_DIR}/{params.name}/var/file_price_{params.name}_{params.name_csv}.csv"

        # Загрузка CSV-файла
        response = requests.get(url)
        response.raise_for_status()  # Проверка успешности запроса

        # Сохранение файла на диск
        with open(file_path, 'wb') as file:
            file.write(response.content)


        try:
            response = requests.get(url, timeout=10)  # Таймаут на случай зависания запроса
            response.raise_for_status()  # Проверяем успешность запроса
            
            # Проверяем, не пустой ли файл
            if not response.content:
                print_log("Ошибка: Файл пустой!")
                # log(msg)
                exit(1)  # Завершаем выполнение с кодом ошибки

            # Сохранение файла
            with open(file_path, "wb") as file:
                file.write(response.content)

            print_log(f"✅ Файл успешно загружен и сохранён: {file_path}")


        except (requests.exceptions.RequestException, ValueError) as e:
            print_log(f"Ошибка загрузки: {e}")
            exit(1)  # Выход с кодом ошибки

            # Если файл уже существует, спрашиваем, продолжать ли с локальной версией
            if os.path.exists(file_path):
                user_input = input("Использовать локальный файл? (y/n): ").strip().lower()
                if user_input != "y":
                    print_log("⛔ Операция отменена пользователем.")
                    exit(1)
                else:
                    print_log(f"📂 Используется локальный файл: {file_path}")
            else:
                print_log("❌ Локальный файл отсутствует. Невозможно продолжить.")
                exit(1)
        price_file_path = file_path

    else: 



        # Формируем путь к прайс-листу
        price_file_path = f"{ROOT_DIR}/{params.name}/var/{params.file_price}"

     # Чтение прайс-листа
    price_df = pd.read_csv(price_file_path, dtype=str)





    if 'countown' in price_df.columns:

        # Чтение распределения городов
        city_distribution_file = f"{ROOT_DIR}/{params.name}/{params.k_gorod}"
        # print(city_distribution_file)
        city_distribution = txt.read_city_distribution(str(city_distribution_file), params.num_ads)
        cities_csv_path = txt.write_city_list_csv(params, ROOT_DIR, shuffle=False, logger=print_log)
        print_log(f"CSV со списком городов создан: {cities_csv_path}")

        print_log(f"Дублирование строк  для аккаунта : {params.name} - {params.name_csv}")
        # Дублирование строк

        extended_price_df = txt.duplicate_rows_robust(price_df, params.num_ads, city_distribution)
        # extended_price_df = txt.duplicate_rows(price_df, params.num_ads, city_distribution)
        rows, cols = extended_price_df.shape
        print(f"Строк: {rows}, Столбцов: {cols}")
        print_log(f"Строк: {rows}, Столбцов: {cols}")
    
        extended_price_df = txt.replace_grand_values(extended_price_df)
    else:
        extended_price_df = price_df
    # print_log(f"Обработка Title для аккаунта : {params.name} - {params.name_csv}")
    # Обработка Titlr
    # if 'Title' in extended_price_df.columns:
    #     extended_price_df = txt.create_and_process_title(params, extended_price_df, ROOT_DIR)


    if 'temp_unik_Description' in extended_price_df.columns:
        print_log(f"Обработка текстов для аккаунта : {params.name} - {params.name_csv}")
        # Обработка текстов + вставка контактов
        extended_price_df = txt.create_and_process_unik_text(params, extended_price_df, ROOT_DIR)
    



    if 'temp_Description' in extended_price_df.columns:
        print_log(f"Обработка текстов для аккаунта : {params.name} - {params.name_csv}")
        # Обработка текстов + вставка контактов
        extended_price_df = txt.create_and_process_text(params, extended_price_df, ROOT_DIR)
    


    # временый колхозный переключатель 
    # (если после генерации картинок и файла необходимо исправить/перегенировать текст сам текст)
    # если 11 создаем сами картинки / если 1 то бе6з картинок но ссылки создаются
    k = 1   #1 c img  / 11 только ссылки 2900 priv

    img_gen = (k == 11)
    if img_gen:
        print_log(f" генерация только текста")
    else:
        print_log(f"  генерация файла и картинок  ")        
    check_time(start_time)
    
    print_log(f" обработка картинок : {params.name} - {params.name_csv}")    
    extended_price_df = txt.create_and_process_img_url(params, extended_price_df, ROOT_DIR, img_gen)
    check_time(start_time)





    if 'DateBegin' in extended_price_df.columns:
        print_log(f"Обработка дат + проверка час пояса для аккаунта : {params.name} - {params.name_csv}")
        # Обработка дат + проверка час пояса
        extended_price_df = txt.create_and_process_date(params, extended_price_df)

    if 'countown' in extended_price_df.columns:

        print_log(f"Обработка id для аккаунта : {params.name} - {params.name_csv}") 
        # Обработка id
        extended_price_df = txt.create_and_process_id(params, extended_price_df)
    check_time(start_time)

    if 'Address' in extended_price_df.columns:
        print_log(f"Обработка адресов для аккаунта : {params.name} - {params.name_csv}")
        # Обработка адресов
        extended_price_df = txt.create_and_process_adres(params, extended_price_df)


    del_col_path = f"{ROOT_DIR}/{params.name}/var/del_col.txt"

    # Чтение названий столбцов для удаления из файла
    columns_to_delete = txt.read_columns_to_delete(del_col_path)

    # Удаление указанных столбцов из DataFrame
    extended_price_df = txt.delete_columns(extended_price_df, columns_to_delete)

    extended_price_df["numad"] = range(len(extended_price_df))

    # Формирование пути для сохранения нового прайс-листа (создается сверху чтобы удалять)
    # output_file_path = f"{ROOT_DIR}/{params.name}/{params.name}_{params.name_csv}_{params.date_f}_{params.num_ads}.csv"
    
    # Сохранение нового прайс-листа
    extended_price_df.to_csv(output_file_path, index=False)

    print_log(f"файл находится по адресу: {output_file_path}")

    check_time(start_time)

    # if __name__ == "__main__":
    # Запуск теста при прямом выполнении модуля
    print("Запуск теста модуля avito_xml_builder...")
    # test_result = txml.test_with_sample_data()
    print("\n✅ Тест завершен. Проверьте файл test_output.xml")
    # txml.customize_for_your_project()

    
    # Сохраняем XML
    # from avito_xml_builder import save_avito_xml_to_file
    xml_path = txml.save_avito_xml_to_file(extended_price_df, params, output_file_path)

    st_par = {
        'name' : params.name, #имя клиента и его папки
        'name_kont' : 'Контакты', #
        'name_pros' : 'Просмотры', #
        'name_izbr' : 'Добавили в избранное', #

        'kont' : 0, #конактов больеше чем это число
        'izbr' : 0, #конактов больеше чем это число
        'pros' : 0, #просмотров больше чем это число

        'name_idstat' : 'Номер объявления', #
        'name_idads' : 'AvitoId', #

        'file_stat' : f"{ROOT_DIR}/{params.name}/stat/stat_{params.name_csv}.xlsx", #
        'file_ads' : f"{ROOT_DIR}/{params.name}/stat/ads_{params.name_csv}.xlsx", #
        'file_before_ads' : f"{ROOT_DIR}/{params.name}/stat/before_ads_{params.name_csv}.xlsx", #
        'filtered_ads' : f'filtered_{params.name_csv}_ads.csv' #

    }


    
    # extended_price_df.to_excel(st_par['file_before_ads'], index=False)
    check_time(start_time)
    print_log(f"Создаю HTML для проверки")
    # df = pd.read_csv('path_to_your_csv_file.csv')  # Загружаем DataFrame
    output_path = f'{ROOT_DIR_OUT}/{params.name}_{params.name_csv}_output.html'  # Путь к файлу, куда будет сохранен HTML
    txt.generate_html_from_df(extended_price_df, output_path)  # Создаем HTML страницу
    check_time(start_time)


    # print_log(f"Создаю прайс для загрузки вордпресс")
    # df = pd.read_csv('path_to_your_csv_file.csv')  # Загружаем DataFrame
    # output_path = f'{ROOT_DIR_OUT}/wp/{params.name}_{params.name_csv}.csv'  # Путь к файлу, куда будет сохранен HTML
    # txt.generate_wp_from_df(extended_price_df, output_path, params)  # Создаем HTML страницу


    # print(vars(params))

# Конец выполнения программы
end_time = time.time()



# Время выполнения программы
execution_time = end_time - start_time
# print(f"Время выполнения программы: {int((execution_time:.2f)/60)} минут ({execution_time:.2f} секунд)")
print_log(format_execution_time(execution_time))
print_log(f"Время выполнения программы: {execution_time:.2f} секунд")
# Текущее время
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print_log(f"Текущее время: {current_time}")
print(f"Текущее время: {current_time}")







if __name__ == "__main__":
    print("Конец.")
else:
    print("my_module.py has been imported.")

