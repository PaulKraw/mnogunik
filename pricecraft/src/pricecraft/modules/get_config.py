import csv
import pandas as pd
import hashlib
import requests
import os
import sys
import time
import math
import random
import openpyxl
import logging
import traceback
from pathlib import Path



import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import gspread
# from google.oauth2.service_account import Credentials

from oauth2client.service_account import ServiceAccountCredentials

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) 
SERVICE_ACCOUNT_FILE = os.path.join(PROJECT_ROOT, "linear-encoder-242307-3cda4e8cf296.json")




import json
from datetime import datetime


from config import settings
PROJECT_ROOT = settings.PROJECT_ROOT
RUNNERS_DIR = os.path.join(PROJECT_ROOT, 'runners')
STATUS_FILE = os.path.join(RUNNERS_DIR, 'buttons_status.json')

# STATUS_FILE = Path("buttons_status.json")
def finish(action):
    with open(STATUS_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    data[action]["status"] = "done"
    data[action]["generated_at"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# def finish(action):
#     data = json.loads(STATUS_FILE.read_text(encoding='utf-8'))
#     data[action]["status"] = "done"
#     data[action]["generated_at"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#     STATUS_FILE.write_text(
#         json.dumps(data, ensure_ascii=False, indent=2),
#         encoding='utf-8'
#     )

# Авторизация через сервисный аккаунт
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)

# Открываем таблицу по URL или ID
SPREADSHEET_ID = "1fLrruYkw0JOOszb6q4bUpNYImflxFkdem57pQPc0qnQ"
sheet = client.open_by_key(SPREADSHEET_ID)

# sheet_name = "final_table"
# worksheet = sheet.worksheet(sheet_name)

# worksheet = sheet.get_worksheet(1800723355) 

# worksheet.clear()

# Настраиваем логгер
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("run_log.txt", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)



def down_respons(url_batya, gideon, name_table):
    name_table = f"csv/{name_table}"
    url = f"{url_batya}/export?format=csv&gid={gideon}"

    response = requests.get(url, verify=False)
    response.raise_for_status() 

    with open(name_table, 'wb') as file:
        file.write(response.content)

    # df = pd.read_csv(url) #болеее

    # Или для всех столбцов
    

    df = pd.read_csv(name_table)

    # df = df.applymap(lambda x: f'{x:.0f}' if pd.notna(x) and isinstance(x, float) and x == int(x) else x)
    df = df.apply(lambda col: col.map(lambda x: f'{x:.0f}' if pd.notna(x) and isinstance(x, float) and x == int(x) else x))


    return df

def down_respons_main(url_batya, gideon, name_table, strok, stolb, rgb_cell):
    url = f"{url_batya}/export?format=xlsx&gid={gideon}"

    # Загружаем Excel файл по URL
    response = requests.get(url, verify=False)
    response.raise_for_status()

    # Сохраняем файл на диск
    with open(name_table, 'wb') as file:
        file.write(response.content)

    # Загружаем Excel файл с помощью openpyxl
    wb = openpyxl.load_workbook(name_table, data_only=True)
    sheet = wb.active  # Получаем активный лист

    data = []

    # Итерируем по строкам, начиная с 13-й (индекс 12) и столбцам, начиная с 5-го (индекс 4)
    for row in sheet.iter_rows(min_row=strok, min_col=stolb):
        row_data = []
        for cell in row:
            # Проверяем цвет ячейки
            # print(f" cell.value {cell.value} cell.fill {cell.fill}")
            
            fill = cell.fill

            color = fill.start_color.rgb

            # color2 = None

            # if cell.value == 77250:
                # print(f"{color} {cell.value}")
                # print(type(color))
                # print(repr(color))
            
            # color2 = color

            # коричневый FFE599, голубой 00FFFF
            # print(color)
            
             #по умолчанию в сравнении глюк, лучше обрезать альфа канал у обоих
            if isinstance(color, str):
                # Обрезаем альфа-канал, если он есть
                rgbcolor = color[2:]
            else:
                # Устанавливаем значение по умолчанию (например, белый)
                rgbcolor = "FFFFFF"  # или None, если так логика требует
            # rgb_cell
            
            # Проверяем, является ли значение числом и ячейка голубая
            # if rgbcolor == '00FFFF':  # Голубой цвет в формате RGBA
            if rgbcolor == rgb_cell:  # Голубой цвет в формате RGBA
                row_data.append(cell.value)  # Если цвет голубой, записываем пустую ячейку
            else:
                if isinstance(cell.value, (int, float)) or cell.value=="#REF!":
                    row_data.append(None)
                else:
                    row_data.append(cell.value)


            # Если цвет ячейки голубой (проверка цвета, например для голубого)

            # row_data.append(cell.value)

            # if color == 'FF00FFFF':  # Пример для голубого (просто для примера, может быть другой код)
            #     row_data.append(None)  # Пропускаем, если цвет не голубой
            # else:
            #     row_data.append(cell.value)


        data.append(row_data)
    
    # Преобразуем отфильтрованные данные в DataFrame для дальнейшей обработки, если нужно
    import pandas as pd
    df = pd.DataFrame(data)
    # print(df)

     # Сохраняем DataFrame в CSV файл
    # df.to_csv("csv/df_check.csv", index=False)

    # Сохраняем DataFrame в новый Excel файл
    # df.to_excel("xls/df_check.xlsx", index=False, header=False)


    return df


def get_df(name_table):

 

    df = pd.read_csv(name_table)

    # df = df.applymap(lambda x: f'{x:.0f}' if pd.notna(x) and isinstance(x, float) and x == int(x) else x)
    df = df.apply(lambda col: col.map(lambda x: f'{x:.0f}' if pd.notna(x) and isinstance(x, float) and x == int(x) else x))


    return df

def get_fixed_hash(input_string, length=32, prefix="oz_"):
    
    # print(f"len(input_string) - {len(input_string) }")
    if len(input_string) > 1:
        # Создаём хеш-объект с алгоритмом md5
        # print("Создаём хеш-объект с алгоритмом md5")
        hash_object = hashlib.md5(input_string.encode())
        # Получаем хеш в шестнадцатеричном виде
        full_hash = hash_object.hexdigest()
        return f"{prefix}{full_hash[:length]}"
    else:
        return input_string.replace(" ", "_")


def add_column_to_dataframe(df_orig, df_merg_path, var_kom, var_kom_obsh):
    """
    Функция для добавления столбца из другой таблицы в исходный DataFrame.

    Parameters:
    df_orig (pd.DataFrame): Исходный DataFrame, в который добавляется новый столбец.
    df_merg_path (str): Путь к CSV файлу для загрузки дополнительной таблицы.
    var_kom (str): Строка, которая будет использоваться для добавления в название нового столбца.
    var_kom_obsh (str): Имя столбца из таблицы df_merg, которое будет использоваться для добавления в новый столбец.
    new_name_table название нового файла

    Returns:
    pd.DataFrame: Обновленный DataFrame с добавленным столбцом.
    """
    # Чтение данных из CSV файла
    df_merg = pd.read_csv(df_merg_path)
    
    # перенесено в ранюю обработку, очистка оригинала вместо очистки каждого
    # # Удаляем строки с пустыми значениями в столбце 'ОЗОН. Вариации.'
    # df_merg = df_merg.dropna(subset=['ОЗОН. Вариации.'])    
    
   

    # Создаем новый столбец в df_orig с именем из var_kom и var_kom_obsh
    df_orig[f"{var_kom}_ar"] = ""
    
    # Новый пустой DataFrame для накопления данных
    df_new = pd.DataFrame(columns=df_orig.columns)

    # Перебор строк df_merg и добавление данных из него в df_orig
    for index, row in df_merg.iterrows():
        # Создаем копию df_orig и добавляем столбец с соответствующим значением
        df_orig_concat = df_orig.copy()  # Используем .copy() для избежания ошибки SettingWithCopyWarning
        df_orig_concat[f"{var_kom}_ar"] = row[var_kom_obsh]

        # Добавляем строки в новый DataFrame
        df_new = pd.concat([df_new, df_orig_concat], ignore_index=True)

    # Возвращаем новый DataFrame с добавленными строками

    # df_new.to_csv(new_name_table, index=False, encoding="utf-8" )
    # Вывод результата (можно также сохранить результат в файл)
    # print(df_new)

    return df_new



def merge_oz_param(df_orig, name_table):

    df_merg = get_df(f"unique_types/{name_table}.csv")

    # df_merged_param = df_orig
    
    df_orig[f"{name_table}_ar"] = df_orig[f"{name_table}_ar"].astype(str)
    df_merg['Имя в базе'] = df_merg['Имя в базе'].astype(str)

    df_merged_param = df_orig.merge(
        df_merg,
        left_on=f"{name_table}_ar",
        right_on='Имя в базе',
        how='left',
        suffixes=("", f"_{name_table}")
    )
    
    # df_merged_param = df_orig.merge(df_merg, left_on=f"{name_table}_ar", right_on='Имя в базе', how='left',suffixes=(f"", f"_{name_table}"))

    # прибавляем цену к общей цене "Сумма" прибавляем цену "цена" комлектующего 
    # суммирование происходит в следующей переборке строк, где формируется артикул общий и хеш артикула общего

    # df_merged_param.to_csv(f"csv/hashed_data_psu_oz_par_after_{name_table}.csv", index=False, encoding="utf-8")

    # отдаем таблицу с одабвлеными параметрами
    return df_merged_param



def merge_prices(df_orig, name_table):
    # Загружаем таблицу
    df_merg = get_df(f"unique_types/{name_table}.csv")

    # Берём только колонку "Имя в базе" и "Цена" (если она есть)
    cols_to_merge = ["Имя в базе"] + [col for col in df_merg.columns if col == "цена"]
    df_merg = df_merg[cols_to_merge]
    
    if "цена" in df_merg.columns:
        df_merg = df_merg.rename(columns={"цена": f"цена_{name_table}"})

    # Выполняем объединение по ключу
    df_merged_param = df_orig.merge(
        df_merg,
        left_on=f"{name_table}_ar",
        right_on="Имя в базе",
        how="left"
    )
    
    if "Имя в базе" in df_merged_param.columns:
        df_merged_param = df_merged_param.drop(columns=["Имя в базе"])
    # df_merged_param.drop(columns=["Имя в базе"]),
    # Возвращаем объединённую таблицу
    return df_merged_param



def print_log(msg):
    with open("log.txt", "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

def init_df(df):
    # df = df.iloc[]
    df = df.dropna(how='all') 
    df = df.dropna(axis=1, how='all') 

    # print(df) 
    # df_new = df.iloc[3:]
    # df = df.iloc[strok-1:, stolb-1:]  # строки с 1 до 3, столбцы с 0 до 2

    df = df.drop(1, axis=0).drop(df.columns[[1,2]], axis=1).reset_index(drop=True)
    df.columns = range(df.shape[1]) 

    df.columns = df.iloc[0]  # Устанавливаем первую строку как названия столбцов
    df = df[1:].reset_index(drop=True)  # Удаляем первую строку и сбрасываем индексы
    return df


def init_conf_df(df):
    # df = df.iloc[]
    df = df.dropna(how='all') 
    df = df.dropna(axis=1, how='all') 

    # print(df) 
    # df_new = df.iloc[3:]
    # df = df.iloc[strok-1:, stolb-1:]  # строки с 1 до 3, столбцы с 0 до 2

    # df = df.drop(1, axis=0).drop(df.columns[[1,2]], axis=1).reset_index(drop=True)
    df.columns = range(df.shape[1]) 

    df.columns = df.iloc[0]  # Устанавливаем первую строку как названия столбцов
    df = df[1:].reset_index(drop=True)  # Удаляем первую строку и сбрасываем индексы
    return df

def getHashTable(df):
    # print(len(df.columns))  # 28 столбцов + 1 первый
    # print(len(df))  # 36 строк + 1 первая


    len_stolb = len(df.columns) 
    len_strok = len(df) 

    data = []

    numstrok = 0

    for i in range(1, len_strok):

        for j in range(1, len_stolb):
            # if j < 5:
            #     print(f" i - {i} / j - {j}")
            # if 
            # print(f"i {i}, j {j}, ")
            value = df.iloc[i, j]
            # if value != "" and pd.notna(value):
            if pd.notna(value):
                numstrok += 1
                stolb_name = df.columns[j]
                # print(f" stolb_name = - {df.columns[j] } ")

                # if j < 5:
                #     print(f" 1 stolb_name = - {df.columns[j] } ")
                # stolb_name = df.iloc[0, j] # Берем имя видеокарты из первой строки

                
                # stolb_name = df.iloc[-1, j]
                # strok_name = df.iloc[i, 0]
                strok_name = df.iloc[i, 0]
                # if j < 5:
                #     print(f" strok_name = - {df.iloc[i, 0]} ")

                
                # print(f"numstrok {numstrok} / i {i}, j {j}, stolb {stolb_name} / strok {strok_name}  val: {value}")

                fixed_hash = get_fixed_hash(f"{strok_name}_{stolb_name}", length=8)

                data.append({"conf":fixed_hash, "Процессор_ar":strok_name,"Видеокарта_ar":stolb_name,"Сумма":value})
        
    df_out = pd.DataFrame(data)

    return df_out





def create_table(df_out,df_vk_bp,df_price_kompl,values):
    print(f"df_vk_bp Строк: {df_vk_bp.shape[0]}, столбцов: {df_vk_bp.shape[1]}")

    df_out_psu = df_out.merge(df_vk_bp[['Видеокарта_ar', 'Питание_ar']], left_on='Видеокарта_ar', right_on='Видеокарта_ar', how='left')

    # Удаляем строки с пустыми значениями в столбце 'ОЗОН. Вариации.'
    df_price_kompl = df_price_kompl.dropna(subset=['ОЗОН. Вариации.'])
    unique_types = [str(x) for x in df_price_kompl["Тип компл"].unique()]

    # Создаём папку, если её нет
    output_folder = "unique_types"
    os.makedirs(output_folder, exist_ok=True)
    # Создаём отдельные таблицы
    print(f"df_out_psu Строк: {df_out_psu.shape[0]}, столбцов: {df_out_psu.shape[1]}")
    
    # # 1. Заменяем пустые строки и пробелы на NaN
    # df_price_kompl["ОЗОН. Вариации."] = df_price_kompl["ОЗОН. Вариации."].replace(r'^\s*$', None, regex=True)

    # # 2. Преобразуем в число, нечисловые значения станут NaN
    # df_price_kompl["ОЗОН. Вариации."] = pd.to_numeric(df_price_kompl["ОЗОН. Вариации."], errors='coerce')

    # # 3. Убираем строки, где нет числа
    # df_price_kompl = df_price_kompl.dropna(subset=["ОЗОН. Вариации."])

    # # 4. (опционально) фильтруем только числа >= 0, если тебе нужно именно такие
    # df_price_kompl = df_price_kompl[df_price_kompl["ОЗОН. Вариации."] >= 0]

    # # 5. Удаляем сам столбец
    # df_price_kompl = df_price_kompl.drop(columns=["ОЗОН. Вариации."])
    
    for comp_type in unique_types:
        if comp_type in ["прочее", "сборка"]:
            continue
        df_filtered = df_price_kompl[df_price_kompl["Тип компл"] == comp_type]  # Фильтруем по типу

        # Удаляем столбцы без значений
        df_filtered = df_filtered.copy()
        df_filtered.replace("", pd.NA, inplace=True)  # Меняем пустые строки на NaN
        df_filtered = df_filtered.dropna(axis=1, how="all")  # Удаляем пустые столбцы

        df_filtered = df_filtered.drop(['Тип компл', 'Цена из прайса'], axis=1, errors='ignore')

        file_name = f"{output_folder}/{comp_type}.csv"  # Формируем путь
        
        df_filtered.to_csv(file_name, index=False, encoding="utf-8")  # Сохраняем
        print(f"Создан файл: {file_name}, строк: {len(df_filtered)}")

        print(f"end {file_name}")
        print(f" ______")
        
    # наименование столбца с артиклами комплектующих
    var_kom_obsh = "Имя в базе"

    # # Фильтруем строки, где "ОЗОН. Вариации." > 0
    
    # filtered_df = filtered_df.dropna(subset=['ОЗОН. Вариации.'])
    df_price_kompl = df_price_kompl[pd.to_numeric(df_price_kompl["ОЗОН. Вариации."], errors='coerce') > 0]

    
    # Берем только столбец "Тип компл" и убираем дубликаты
    unique_types_var = df_price_kompl["Тип компл"].drop_duplicates().tolist()
    print(f"df_out_psu Строк: {df_out_psu.shape[0]}, столбцов: {df_out_psu.shape[1]}")

    final_table = df_out_psu
    
    for var_kom in unique_types_var:
        # Вызов функции для добавления данных из таблицы
        if var_kom in ["прочее", "сборка"]:
            continue
        final_table = add_column_to_dataframe(final_table, f"unique_types/{var_kom}.csv", var_kom, var_kom_obsh)
        print(f"final_table Строк: {final_table.shape[0]}, столбцов: {final_table.shape[1]}")


    
    # названия комплектующих по которым будем проходить 
    list_par_kom = unique_types


#прибавляет цену????
    for name_table in list_par_kom:
        print(f"обработка комлл - {name_table}")
        final_table = merge_prices(final_table, name_table)
    print(f"final_table Строк: {final_table.shape[0]}, столбцов: {final_table.shape[1]}")


        # Проверим колонки в df_vk_bp
    print("Колонки df_vk_bp:", df_vk_bp.columns.tolist())

    # Если совместимость хранится в 'Видеокарта_ar' и 'Питание_ar' (но для процессора)
    cpu_col_vk_bp = 'Видеокарта_ar'  # на самом деле тут артикулы процессоров
    ram_col_vk_bp = 'Питание_ar'     # на самом деле тут артикулы памяти

    # Создаем множество совместимых пар из df_vk_bp
    compatible_pairs = set(zip(df_vk_bp[cpu_col_vk_bp], df_vk_bp[ram_col_vk_bp]))

    # Сохраняем исходное количество строк
    initial_count = len(final_table)

    # Создаем маску для удаления НЕсовместимых строк
    # True = оставляем (совместимые), False = удаляем (несовместимые)
    mask = final_table.apply(
        lambda row: (row['Процессор_ar'], row['Память_ar']) in compatible_pairs,
        axis=1
    )

    # Удаляем несовместимые строки из final_table
    final_table.drop(final_table[~mask].index, inplace=True)

    # Сбрасываем индексы после удаления
    final_table.reset_index(drop=True, inplace=True)

    print(f"Удалено несовместимых конфигов: {initial_count - len(final_table)}")
    print(f"Осталось совместимых конфигов: {len(final_table)}")

    final_table["full_art_hash"] = final_table.apply(lambda row: get_fixed_hash(f'{row["Процессор_ar"]}_{row["Видеокарта_ar"]}_{row["Питание_ar"]}_{row["Память_ar"]}_{row["Диск_ar"]}_{row["Кейс_ar"]}', 32), axis=1)

    final_table["full_art"] = final_table.apply(lambda row: f'{row["Процессор_ar"]}_{row["Видеокарта_ar"]}_{row["Питание_ar"]}_{row["Память_ar"]}_{row["Диск_ar"]}_{row["Кейс_ar"]}', axis=1)


    # обработка финальной таблицы. 
    # добавление артикула и его хеша  (проц выше вк. то е первее) проц, вк, питание, оп, диск, кейс
    
    # print(f"" )
    for i, row in final_table.iterrows():
        full_art = f'{row["Процессор_ar"]}_{row["Видеокарта_ar"]}_{row["Питание_ar"]}_{row["Память_ar"]}_{row["Диск_ar"]}_{row["Кейс_ar"]}'

        # прибавляем цену к общей цене "Сумма" прибавляем цену "цена" комлектующего 
        new_price = str(
            int(values["sborka"]) +  #сборка
            int(values["proch"]) +  #прочее
            int(row["цена_Память"]) + #
            int(row["цена_Диск"]) + #
            int(row["цена_Кейс"]) + #
            int(row["цена_Процессор"]) + #
            int(row["цена_Видеокарта"]) + #
            int(row["цена_Питание"]) #
        )
        final_table.loc[i, "Сумма итог"] = str(new_price) #округление до сотых

        # создаем столбец название кейса (он есть в столбце кейс подставленый из "цен комплектующих)")
        # столбец формируется выше в функции

        # артикул это хеш полного артикула - Артикул*
        final_table = final_table.rename(columns={"full_art_hash": "articul"})
        



        # Название модели (для объединения в одну карточку)* - использовать для объединения конфигов по типу кейса.

        # Название модели (для объединения в одну карточку)*

        if i % 100 == 0:
            print(f"\r  _ {i} номер строки обаботки цены {i} full_art {full_art}")
        
            # sys.stdout.write(f"\r  _ {i} номер строки обаботки цены {i} full_art {full_art}                                             \r  ")
            # sys.stdout.flush()

        # if i == 0 and i == 7611:
        #     print(f"{i} full_art {full_art} full_art_hash {full_art_hash}")
    
    

    # SERVICE_ACCOUNT_FILE = os.path.join(PROJECT_ROOT, "linear-encoder-242307-3cda4e8cf296.json")
    col_delete = os.path.join(PROJECT_ROOT, "txt/col_delete.txt")
    # получаем список названий столбцов на удаление
    with open(col_delete, "r", encoding="utf-8") as file:
        columns_to_delete = [line.strip() for line in file.readlines()]
    final_table = final_table.drop(columns_to_delete, axis=1, errors='ignore')

    columns_order = os.path.join(PROJECT_ROOT, "txt/columns_order.txt")
    # Читаем список столбцов из текстового файла
    with open(columns_order, "r", encoding="utf-8") as file:
        column_order = [line.strip() for line in file.readlines()]

    # Определяем, какие из этих столбцов есть в DataFrame
    existing_columns = [col for col in column_order if col in final_table.columns]

    # Определяем столбцы, которых нет в списке, но они есть в DataFrame
    remaining_columns = [col for col in final_table.columns if col not in column_order]

    # Переупорядочиваем столбцы: сначала те, что есть в списке, затем остальные
    final_table = final_table[existing_columns + remaining_columns]

    final_table["Категория"] = "игровые пк"
    # final_table["Description_ozon"] = "text.txt"
    # final_table["first_img_ozon"] = "img_first_pk.json"
    # final_table["Description_wb"] = "text.txt"
    # final_table["first_img_wb"] = "img_first_pk.json"
    final_table["Description"] = ""
    final_table["ImageUrls"] = ""
    final_table["temp_Description"] = "text_ozon.txt"
    final_table["first_img"] = "img_first_pk.json"


    return final_table

    # final_table.to_csv(f"csv/{name_df}_fin_table.csv", index=False, encoding="utf-8" )

# старая обработка маппинг параметров из общей "шахаматки"
def ensure_ozon_columns_from_mapping(df_final, mapping):
    # helper: маска пустых ячеек в серии
    def is_empty_series(s):
        return s.isna() | (s.astype(str).str.strip() == "")

    # пройдём по mapping: name -> default (строка или "")
    for col_name, default in mapping.items():
        # нормализуем имя колонки (строка)
        if col_name is None:
            continue
        col_name = str(col_name).strip()

        if col_name == "":
            continue

        if col_name not in df_final.columns:
            # если колонки нет — добавляем и заполняем default или пустой строкой
            df_final[col_name] = default if default is not None else ""
        else:
            # колонка есть — подставляем default только в пустые ячейки, если default непустой
            empty_mask = is_empty_series(df_final[col_name])
            if default != "":
                if empty_mask.any():
                    df_final.loc[empty_mask, col_name] = default
            else:
                # default пустой: заменим NaN на пустую строку (чтобы безопасно выгружать)
                if empty_mask.any():
                    df_final.loc[empty_mask, col_name] = ""

    # заменим оставшиеся NaN на пустые строки и вернём df
    return df_final.fillna("")

def apply_marketplace_mapping(df_out, df_mapping, marketplace):
    df_mp = df_mapping[df_mapping["marketplace"] == marketplace]

    for target_param_name in df_mp["target_param_name"].unique():
        if target_param_name not in df_out.columns:
            df_out[target_param_name] = ""
    
    for _, row in df_mp.iterrows():
        source_param_name = row["source_param_name"]
        source_value = row["source_value"]
        target_param_name = row["target_param_name"]
        target_value = row["target_value"]
        if pd.isna(source_value):
            continue
        if source_param_name not in df_out.columns:
            continue

        mask = df_out[source_param_name] == source_value
        df_out.loc[mask, target_param_name] = target_value
        

    return df_out




# def create_ozon_table(df_out,df_price_kompl,values, ozon_mapping): в новой другой маппинг по параметрам
def create_ozon_table(df_out,df_price_kompl,values, ozon_mapping):
    
    
    unique_types = [str(x) for x in df_price_kompl["Тип компл"].unique()]

    # Создаём папку, если её нет
    output_folder = "unique_types"
    os.makedirs(output_folder, exist_ok=True)
    
    for comp_type in unique_types:
        df_filtered = df_price_kompl[df_price_kompl["Тип компл"] == comp_type]  # Фильтруем по типу

        # Удаляем столбцы без значений
        df_filtered = df_filtered.copy()
        df_filtered.replace("", pd.NA, inplace=True)  # Меняем пустые строки на NaN
        df_filtered = df_filtered.dropna(axis=1, how="all")  # Удаляем пустые столбцы


        df_filtered = df_filtered.drop(['Тип компл', 'Цена из прайса'], axis=1, errors='ignore')
        file_name = f"{output_folder}/{comp_type}.csv"  # Формируем путь
        df_filtered.to_csv(file_name, index=False, encoding="utf-8")  # Сохраняем
        print(f"Создан файл: {file_name}, строк: {len(df_filtered)}")

        
        # print(df_filtered)
        print(f"end {file_name}")
        print(f" ______")

    # for name_table in unique_types:
    #     print(f"обработка комлл - {name_table}")
    #     df_out = merge_oz_param(df_out, name_table)
    df_out = apply_marketplace_mapping(
        df_out=df_out,
        df_mapping=ozon_mapping,
        marketplace="ozon"
    )
    for i, row in df_out.iterrows():
       

        # new_price = str( int(values["Сумма итог"]) * int(values["ozon"])  )
        
        # new_price = int(math.ceil(float(values["Сумма итог"]) * float(values["ozon"]) * 100) / 100)
        new_price = int(math.ceil(float(str(row["Сумма итог"]).replace(',', '.')) * float(str(values.get("ozon", 0)).replace(',', '.')) * 100) / 100)


        df_out.loc[i, "Цена, руб.*"] = str(new_price) #округление до сотых
        
        old_price = int(math.ceil(float(new_price) * float(str(values.get("ozon", 0)).replace(',', '.')) * 100) / 100)
        
        df_out.loc[i, "Цена до скидки, руб."] = str(old_price) #округление до сотых

  
        # костыль названия конфигурации

        
        # Запись случайного варианта в столбец
        df_out.loc[i, "Title"] = getname_sbopk(row)
    
    df_out = df_out.rename(columns={"articul": "Артикул*"})
    df_out = df_out.rename(columns={"Title": "Название товара"})
    df_out = df_out.rename(columns={"Кейс название": "Название модели (для объединения в одну карточку)*"})



    # SERVICE_ACCOUNT_FILE = os.path.join(PROJECT_ROOT, "linear-encoder-242307-3cda4e8cf296.json")
    col_delete = os.path.join(PROJECT_ROOT, "txt/col_delete_ozon.txt")
    # получаем список названий столбцов на удаление
    with open(col_delete, "r", encoding="utf-8") as file:
        columns_to_delete = [line.strip() for line in file.readlines()]
    df_out = df_out.drop(columns_to_delete, axis=1, errors='ignore')



    columns_order = os.path.join(PROJECT_ROOT, "txt/columns_order_ozon.txt")
    # Читаем список столбцов из текстового файла
    with open(columns_order, "r", encoding="utf-8") as file:
        column_order = [line.strip() for line in file.readlines()]

    # Определяем, какие из этих столбцов есть в DataFrame
    existing_columns = [col for col in column_order if col in df_out.columns]

    # Определяем столбцы, которых нет в списке, но они есть в DataFrame
    remaining_columns = [col for col in df_out.columns if col not in column_order]

    # Переупорядочиваем столбцы: сначала те, что есть в списке, затем остальные
    df_out = df_out[existing_columns + remaining_columns]

    return df_out

    

def create_wb_table(df_out,df_price_kompl,values, ozon_mapping):
    
    
    unique_types = [str(x) for x in df_price_kompl["Тип компл"].unique()]

    # Создаём папку, если её нет
    output_folder = "unique_types"
    os.makedirs(output_folder, exist_ok=True)
    
    for comp_type in unique_types:
        df_filtered = df_price_kompl[df_price_kompl["Тип компл"] == comp_type]  # Фильтруем по типу

        # Удаляем столбцы без значений
        df_filtered = df_filtered.copy()
        df_filtered.replace("", pd.NA, inplace=True)  # Меняем пустые строки на NaN
        df_filtered = df_filtered.dropna(axis=1, how="all")  # Удаляем пустые столбцы


        df_filtered = df_filtered.drop(['Тип компл', 'Цена из прайса'], axis=1, errors='ignore')
        file_name = f"{output_folder}/{comp_type}.csv"  # Формируем путь
        df_filtered.to_csv(file_name, index=False, encoding="utf-8")  # Сохраняем
        print(f"Создан файл: {file_name}, строк: {len(df_filtered)}")

        
        # print(df_filtered)
        print(f"end {file_name}")
        print(f" ______")

    for name_table in unique_types:
        print(f"обработка комлл - {name_table}")
        df_out = merge_oz_param(df_out, name_table)

    for i, row in df_out.iterrows():
       

        # new_price = str( int(values["Сумма итог"]) * int(values["ozon"])  )
        
        # new_price = int(math.ceil(float(values["Сумма итог"]) * float(values["ozon"]) * 100) / 100)
        new_price = int(math.ceil(float(str(row["Сумма итог"]).replace(',', '.')) * float(str(values.get("wb", 0)).replace(',', '.')) * 100) / 100)


        df_out.loc[i, "Цена"] = str(new_price) #округление до сотых
        
        old_price = int(math.ceil(float(new_price) * float(str(values.get("wb", 0)).replace(',', '.')) * 100) / 100)
        
        df_out.loc[i, "Цена до скидки, руб."] = str(old_price) #округление до сотых

  
        # костыль названия конфигурации

        
        # Запись случайного варианта в столбец
        df_out.loc[i, "Title"] = getname_sbopk(row)
        
#         Наименование
# Описание
# Фото
# Цена
# Артикул OZON

    # return df_out
    # print("" )
    
    df_out = df_out.rename(columns={"articul": "Артикул продавца"})
    df_out = df_out.rename(columns={"Title": "Наименование"})
    df_out = df_out.rename(columns={"Кейс название": "Модель"})


    df_out = apply_marketplace_mapping(
        df_out=df_out,
        df_mapping=ozon_mapping,
        marketplace="wb"
    )
    # SERVICE_ACCOUNT_FILE = os.path.join(PROJECT_ROOT, "linear-encoder-242307-3cda4e8cf296.json")
    col_delete = os.path.join(PROJECT_ROOT, "txt/col_delete_ozon.txt")
    # получаем список названий столбцов на удаление
    with open(col_delete, "r", encoding="utf-8") as file:
        columns_to_delete = [line.strip() for line in file.readlines()]
    df_out = df_out.drop(columns_to_delete, axis=1, errors='ignore')



    columns_order = os.path.join(PROJECT_ROOT, "txt/columns_order_wb.txt")
    # Читаем список столбцов из текстового файла
    with open(columns_order, "r", encoding="utf-8") as file:
        column_order = [line.strip() for line in file.readlines()]

    # Определяем, какие из этих столбцов есть в DataFrame
    existing_columns = [col for col in column_order if col in df_out.columns]

    # Определяем столбцы, которых нет в списке, но они есть в DataFrame
    remaining_columns = [col for col in df_out.columns if col not in column_order]

    # Переупорядочиваем столбцы: сначала те, что есть в списке, затем остальные
    df_out = df_out[existing_columns + remaining_columns]

    return df_out


def load_ozon_mapping(sheet, sheet_name="ozon_mapping"):
    ws = sheet.worksheet(sheet_name)
    records = ws.get_all_records()
    return pd.DataFrame(records)

def main():
    logging.info("Скрипт запущен")
    start_time = time.time()

    #создаем папку если ее нет
    os.makedirs("csv", exist_ok=True)
  
    df_blu_out.to_csv("csv/df_blu_out_hashed_data.csv", index=False, encoding="utf-8")




def getname_sbopk(row):
    try:
        my_list = ["Игровой компьютер", "Игровой ПК"]
        cp_list = [
            f"{row['Число ядер процессора']}х{row['Частота процессора, ГГц']} ГГц",
            "",
        ]

        # Случайный выбор значений
        title_type = random.choice(my_list)
        cpu_info = random.choice(cp_list)

        # Формирование названий
        name_conf = (
            f"{title_type} ULTRAFPS ({row['Видеокарта*']}, {row['Процессор*']} {cpu_info}, "
            f"{row['Оперативная память*']}, {row['Диск ГБ']} ГБ, {row['Кейс название']})"
        )

        name_conf2 = (
            f"{title_type} {row['Кейс название']} ULTRAFPS ({row['Видеокарта*']}, "
            f"{row['Процессор*']} {cpu_info}, {row['Оперативная память*']}, {row['Диск ГБ']} ГБ)"
        )

        return random.choice([name_conf, name_conf2])
    except KeyError:   
        return "Игровой компьютер"





# def load_status():
#     return json.loads(STATUS_FILE.read_text(encoding="utf-8"))


# def save_status(data):
#     STATUS_FILE.write_text(
#         json.dumps(data, ensure_ascii=False, indent=2),
#         encoding="utf-8"
#     )


# def set_generating(action):
#     data = load_status()

#     data["global"]["is_generating"] = True
#     data["buttons"][action]["status"] = "generating"
#     data["buttons"][action]["generated_at"] = None

#     save_status(data)


# def set_done(action):
#     data = load_status()

#     data["buttons"][action]["status"] = "done"
#     data["buttons"][action]["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#     # если больше никто не генерируется
#     data["global"]["is_generating"] = any(
#         btn["status"] == "generating"
#         for btn in data["buttons"].values()
#     )

#     save_status(data)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("Произошла ошибка:")
        logging.error(traceback.format_exc())  

        print("Произошла ошибка! Подробности в run_log.txt")
        input("Нажмите Enter, чтобы выйти...")  # оставляет окно открытым
    else:
        logging.info("Скрипт завершён без ошибок.")
        input("Нажмите Enter, чтобы выйти...")


