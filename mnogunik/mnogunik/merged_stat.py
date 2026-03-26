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
import textfun as txt
import statfun as stt
from klass import ClientParams


start_time = time.time()

ROOT_DIR = 'D:/proj/'

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


params_list = txt.read_params_from_csv('cl_rows.csv')




for params in params_list:
    print(f"аккаунт : {params.name} - {params.name_csv}")
    # Формируем путь к прайс-листу
    price_file_path = f"{ROOT_DIR}/{params.name}/var/{params.file_price}"

     # Чтение прайс-листа
    price_df = pd.read_csv(price_file_path, dtype=str)

    # Чтение распределения городов
    city_distribution_file = f"{ROOT_DIR}/{params.name}/{params.k_gorod}"
    # print(city_distribution_file)
    city_distribution = txt.read_city_distribution(str(city_distribution_file), params.num_ads)
 
    print(f"Дублирование строк  для аккаунта : {params.name} - {params.name_csv}")
    # Дублирование строк
    extended_price_df = txt.duplicate_rows(price_df, params.num_ads, city_distribution)
    
    print(f"Обработка текстов для аккаунта : {params.name} - {params.name_csv}")
    # Обработка текстов + вставка контактов
    extended_price_df = txt.create_and_process_text(params, extended_price_df, ROOT_DIR)
    
    print(f"Обработка Title для аккаунта : {params.name} - {params.name_csv}")
    # Обработка Titlr
    extended_price_df = txt.create_and_process_title(params, extended_price_df, ROOT_DIR)

    # print(extended_price_df)

    print(f"  - - - -Картинки для аккаунта : {params.name} - {params.name_csv}")
    # extended_price_df = txt.create_and_process_img_url(params, extended_price_df, ROOT_DIR, True)
    extended_price_df = txt.create_and_process_img_url(params, extended_price_df, ROOT_DIR, False)



    print(f"Обработка дат + проверка час пояса для аккаунта : {params.name} - {params.name_csv}")
     # Обработка дат + проверка час пояса
    extended_price_df = txt.create_and_process_date(params, extended_price_df)


    print(f"Обработка id для аккаунта : {params.name} - {params.name_csv}")
     # Обработка id
    extended_price_df = txt.create_and_process_id(params, extended_price_df)


    print(f"Обработка адресов для аккаунта : {params.name} - {params.name_csv}")
     # Обработка адресов
    extended_price_df = txt.create_and_process_adres(params, extended_price_df)


    del_col_path = f"{ROOT_DIR}/{params.name}/var/del_col.txt"

    # Чтение названий столбцов для удаления из файла
    columns_to_delete = txt.read_columns_to_delete(del_col_path)

    # Удаление указанных столбцов из DataFrame
    extended_price_df = txt.delete_columns(extended_price_df, columns_to_delete)

    # Формирование пути для сохранения нового прайс-листа
    output_file_path = f"{ROOT_DIR}/{params.name}/{params.name}_{params.name_csv}_{params.date_f}_{params.num_ads}.csv"
    
    # Сохранение нового прайс-листа
    extended_price_df.to_csv(output_file_path, index=False)
    
    st_par = {
        'name' : params.name, #имя клиента и его папки
        'name_kont' : 'Запросов контактов на Avito', #
        'name_pros' : 'Просмотров на Avito', #
        'name_izbr' : 'Добавлений в избранное на Avito', #

        'kont' : 0, #конактов больеше чем это число
        'izbr' : 0, #конактов больеше чем это число
        'pros' : 0, #просмотров больше чем это число

        'name_idstat' : 'Номер в Avito', #
        'name_idads' : 'AvitoId', #

        'file_stat' : f"{ROOT_DIR}/{params.name}/stat/stat_{params.name_csv}.xlsx", #
        'file_ads' : f"{ROOT_DIR}/{params.name}/stat/ads_{params.name_csv}.xlsx", #
        'filtered_ads' : f'filtered_{params.name_csv}_ads.csv' #

    }

    # if os.path.exists(st_par['file_stat']):
    if os.path.exists(st_par['file_stat']) and os.path.exists(st_par['file_ads']):
        try:
            # stat_data = pd.read_excel(file_path)
            print('начало обработки статистики')
            print('stt.kick_nulstat(st_par, params)')
            stt.kick_nulstat(st_par, params)

            print("приступаю к объединению")

            # Пример использования
            csv_filename1 = output_file_path
            csv_filename2 = f"{ROOT_DIR}/{params.name}/stat/{st_par['filtered_ads']}"  # замените на фактическое имя второго файла
            output_filename_stat = f"{ROOT_DIR}/{params.name}/statads_{params.name}_{params.name_csv}_{params.date_f}_{params.num_ads}.csv"

            txt.merge_csv_files(csv_filename1, csv_filename2, output_filename_stat)

            txt.clean_merged_data(output_filename_stat)

        except Exception as e:
            print(f"Произошла ошибка при считывании файла: {e}")
            print(f"1 / Файл {st_par['file_stat']} не найден. программа создаст только новые объявления!")
            # sys.exit(1)  # Завершение программы с кодом 1 (ошибка)
    
    else:
        print(f"2 / Файл {st_par['file_stat']} не найден. программа создаст только новые объявления!")
        # sys.exit(1)  # Завершение программы с кодом 1 (ошибка)                                          
        # Вывод параметров для отладки

    print(f"Создаю HTML для проверки")
    # df = pd.read_csv('path_to_your_csv_file.csv')  # Загружаем DataFrame
    output_path = f'D:/proj/outfile/{params.name}_output.html'  # Путь к файлу, куда будет сохранен HTML
    txt.generate_html_from_df(extended_price_df, output_path)  # Создаем HTML страницу


    print(vars(params))

# Конец выполнения программы
end_time = time.time()

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

# Время выполнения программы
execution_time = end_time - start_time
# print(f"Время выполнения программы: {int((execution_time:.2f)/60)} минут ({execution_time:.2f} секунд)")
print(format_execution_time(execution_time))
print(f"Время выполнения программы: {execution_time:.2f} секунд")
# Текущее время
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Текущее время: {current_time}")





if __name__ == "__main__":
    print("Конец.")
else:
    print("my_module.py has been imported.")

