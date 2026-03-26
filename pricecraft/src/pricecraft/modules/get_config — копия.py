import csv
import pandas as pd
import hashlib
import requests
import os
import sys
import time
import datetime
import math
import random
import openpyxl
import logging
import traceback
from pathlib import Path


import gspread
# from google.oauth2.service_account import Credentials

from oauth2client.service_account import ServiceAccountCredentials

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__)) 
SERVICE_ACCOUNT_FILE = os.path.join(PROJECT_ROOT, "linear-encoder-242307-3cda4e8cf296.json")

# Авторизация через сервисный аккаунт
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)

# Открываем таблицу по URL или ID
SPREADSHEET_ID = "1fLrruYkw0JOOszb6q4bUpNYImflxFkdem57pQPc0qnQ"
sheet = client.open_by_key(SPREADSHEET_ID)

sheet_name = "final_table"
worksheet = sheet.worksheet(sheet_name)

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


def add_column_to_dataframe(df_orig, df_merg_path, var_kom, var_kom_obsh, new_name_table):
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

    df_new.to_csv(new_name_table, index=False, encoding="utf-8" )
    # Вывод результата (можно также сохранить результат в файл)
    print(df_new)

    return df_new



def merge_oz_param(df_orig, name_table):

    df_merg = get_df(f"unique_types/{name_table}.csv")

    # df_merged_param = df_orig
    
    df_merged_param = df_orig.merge(df_merg, left_on=f"{name_table}_ar", right_on='Имя в базе', how='left',suffixes=(f"", f"_{name_table}"))

    # прибавляем цену к общей цене "Сумма" прибавляем цену "цена" комлектующего 
    # суммирование происходит в следующей переборке строк, где формируется артикул общий и хеш артикула общего

    df_merged_param.to_csv(f"csv/hashed_data_psu_oz_par_after_{name_table}.csv", index=False, encoding="utf-8")

    # отдаем таблицу с одабвлеными параметрами
    return df_merged_param

def print_log(msg):
    with open("log.txt", "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)


def main():
    logging.info("Скрипт запущен")
    start_time = time.time()

    #создаем папку если ее нет
    os.makedirs("csv", exist_ok=True)
 

    url_batya = 'https://docs.google.com/spreadsheets/d/1fLrruYkw0JOOszb6q4bUpNYImflxFkdem57pQPc0qnQ'

    strok = 14 # на это строке размещаются строчные названия
    stolb = 5 # в этом столбце размещаются столбчатые названия
    beg_table = [8, 15]

    #оригинальный лист
    # df = down_respons(url_batya,140246863,"cpu_gpu.csv")

    #для озон (голубые)
    # df = down_respons(url_batya,76297089,"cpu_gpu.csv") 

    # 140246863 старый guid листа 
    guid = 72304825 # guid листа в ссылке (когда открыт личт)
    df = down_respons_main(url_batya,guid,"cpu_gpu.xlsx", strok, stolb, "00FFFF")

    # df_del = down_respons_main(url_batya,140246863,"cpu_gpu.xlsx", strok, stolb, "FF0000")
    # df_add = down_respons_main(url_batya,140246863,"cpu_gpu.xlsx", strok, stolb, "00FF00")


    def init_df(df):
        # df = df.iloc[]
        df = df.dropna(how='all') 
        df = df.dropna(axis=1, how='all') 

        print(df) 
        # df_new = df.iloc[3:]
        # df = df.iloc[strok-1:, stolb-1:]  # строки с 1 до 3, столбцы с 0 до 2

        df = df.drop(1, axis=0).drop(df.columns[[1,2]], axis=1).reset_index(drop=True)
        df.columns = range(df.shape[1]) 

        df.columns = df.iloc[0]  # Устанавливаем первую строку как названия столбцов
        df = df[1:].reset_index(drop=True)  # Удаляем первую строку и сбрасываем индексы
        return df

    df= init_df(df)
    # df_del = init_df(df_del)
    # df_add = init_df(df_add)
    


    #создаем папку если ее нет
    os.makedirs("csv", exist_ok=True)


    # print(df) 
    # df_new = df.drop(df.columns[[1,2]], axis=1).reset_index(drop=True)


    # print(df)  # Печатает таблицу df_new

    def getHashTable(df):
        print(len(df.columns))  # 28 столбцов + 1 первый
        print(len(df))  # 36 строк + 1 первая


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

    df_blu_out = getHashTable(df)
    df_blu_out.to_csv("csv/df_blu_out_hashed_data.csv", index=False, encoding="utf-8")

    # df_del_out = getHashTable(df_del)
    # df_del_out.to_csv("csv/df_del_hashed_data.csv", index=False, encoding="utf-8")

    # df_add_out = getHashTable(df_add)
    # df_add_out.to_csv("csv/df_add_hashed_data.csv", index=False, encoding="utf-8")

    # df_out.to_csv("csv/hashed_data.csv", index=False, encoding="utf-8")

  


    def create_table(df_out,name_df):
        # загрузка соответствий блока питания и видеокрты, далее сопряжение - добавление блока питания в сборку

        df_vk_bp = down_respons(url_batya,634015161,"vk_bp.csv")

        # берем и подставляем блок питания согласно таблице
        # без цены так как цена уже заложено в общую стоимость конфигурация

  
        df_out_psu = df_out.merge(df_vk_bp[['Видеокарта_ar', 'Питание_ar']], left_on='Видеокарта_ar', right_on='Видеокарта_ar', how='left')

        df_out_psu.to_csv(f"csv/{name_df}_hashed_data_psu.csv", index=False, encoding="utf-8")

            

        # Загрузка таблиц комплектующих и создание отдельных таблиц ссохраненнием их по типам комплеткующих 

        df_price_kompl = down_respons(url_batya,406620284,"price_kompl.csv")
        # Получаем уникальные значения из столбца "Тип компл"
        # unique_types = df_price_kompl["Тип компл"].unique()
        

        # Удаляем строки с пустыми значениями в столбце 'ОЗОН. Вариации.'
        df_price_kompl = df_price_kompl.dropna(subset=['ОЗОН. Вариации.'])
        unique_types = [str(x) for x in df_price_kompl["Тип компл"].unique()]

        print(f"length {len(unique_types)} \n {unique_types}")

        # Создаём папку, если её нет
        output_folder = "unique_types"
        os.makedirs(output_folder, exist_ok=True)
        # Создаём отдельные таблицы
        for comp_type in unique_types:
            df_filtered = df_price_kompl[df_price_kompl["Тип компл"] == comp_type]  # Фильтруем по типу

            # Удаляем столбцы без значений
            df_filtered = df_filtered.copy()
            df_filtered.replace("", pd.NA, inplace=True)  # Меняем пустые строки на NaN
            df_filtered = df_filtered.dropna(axis=1, how="all")  # Удаляем пустые столбцы

            # df_filtered = df_filtered.dropna(axis=1, how="all")  # Удаляем пустые столбцы


            df_filtered = df_filtered.drop(['Тип компл', 'Цена из прайса'], axis=1, errors='ignore')

            file_name = f"{output_folder}/{comp_type}.csv"  # Формируем путь
            df_filtered.to_csv(file_name, index=False, encoding="utf-8")  # Сохраняем
            print(f"Создан файл: {file_name}, строк: {len(df_filtered)}")




        # наименование столбца с артиклами комплектующих
        var_kom_obsh = "Имя в базе"


        var_kom = "Память"
        df_orig = df_out_psu  # Исходная таблица
        new_name_table = "csv/hashed_data_psu_op.csv"

        # Вызов функции для добавления данных из таблицы
        df_out_psu_op = add_column_to_dataframe(df_orig, f"unique_types/{var_kom}.csv", var_kom, var_kom_obsh, new_name_table)


        var_kom = "Диск"
        df_orig = df_out_psu_op  # Исходная таблица
        new_name_table = "csv/hashed_data_psu_op_disk.csv"
        df_out_psu_op_disk = add_column_to_dataframe(df_orig, f"unique_types/{var_kom}.csv", var_kom, var_kom_obsh, new_name_table)


        var_kom = "Кейс"
        df_orig = df_out_psu_op_disk  # Исходная таблица
        new_name_table = "csv/hashed_data_psu_op_disk_case.csv"
        df_out_psu_op_disk_case = add_column_to_dataframe(df_orig, f"unique_types/{var_kom}.csv", var_kom, var_kom_obsh, new_name_table)

        # названия комплектующих по которым будем проходить 
        list_par_kom = unique_types







        #  = создаем финальную таблицу в которую будем добавлять параметры для озон, по списку комплектующих
        final_table = df_out_psu_op_disk_case

        for name_table in list_par_kom:
            print(f"обработка комлл - {name_table}")
            final_table = merge_oz_param(final_table, name_table)


        # print("обработка финальной таблицы. ")






        final_table["full_art_hash"] = final_table.apply(lambda row: get_fixed_hash(f'{row["Процессор_ar"]}_{row["Видеокарта_ar"]}_{row["Питание_ar"]}_{row["Память_ar"]}_{row["Диск_ar"]}_{row["Кейс_ar"]}', 32), axis=1)

        final_table["full_art"] = final_table.apply(lambda row: f'{row["Процессор_ar"]}_{row["Видеокарта_ar"]}_{row["Питание_ar"]}_{row["Память_ar"]}_{row["Диск_ar"]}_{row["Кейс_ar"]}', axis=1)


        # обработка финальной таблицы. 
        # добавление артикула и его хеша  (проц выше вк. то е первее) проц, вк, питание, оп, диск, кейс
        my_list = ["Игровой компьютер", "Игровой ПК"]
        print(f"" )
        for i, row in final_table.iterrows():
            full_art = f'{row["Процессор_ar"]}_{row["Видеокарта_ar"]}_{row["Питание_ar"]}_{row["Память_ar"]}_{row["Диск_ar"]}_{row["Кейс_ar"]}'
            # full_art_hash = get_fixed_hash(full_art)

            # df_melted["conf"] = df_melted.apply(lambda row: get_fixed_hash(full_art, 10), axis=1)

            # final_table.loc[i, "full_art"] = full_art
            # final_table.loc[i, "full_art_hash"] = full_art_hash

            # прибавляем цену к общей цене "Сумма" прибавляем цену "цена" комлектующего 

            new_price = str(
                int(row["Сумма"]) +  #ожидается число из пересечения процессора и видеокарты (в ней заложена и сборка 1500 и цены на базовые комлектующие)
                int(row["цена_Память"]) + #цена за вычетом цены базовой комплектации
                int(row["цена_Диск"]) + #цена за вычетом цены базовой комплектации
                int(row["цена_Кейс"]) #цена за вычетом цены базовой комплектации
            )
            new_price = math.ceil(float(new_price) * 1.42) #math.ceil округление вверх

            # row["цена_Питание"] row["цена_Память"] row["цена_Диск"] row["цена_Кейс"]
            # final_table.loc[i, "Сумма"] = str(round(new_price, -2)) #округление до сотых
            final_table.loc[i, "Сумма итог"] = str(round(new_price, -2)) #округление до сотых

            # final_table.loc[i, "full_art"] = full_art

            # создаем столбец название кейса (он есть в столбце кейс подставленый из "цен комплектующих)")
            # столбец формируется выше в функции

            # артикул это хеш полного артикула - Артикул*
            final_table = final_table.rename(columns={"full_art_hash": "articul"})

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

            # Запись случайного варианта в столбец
            final_table.loc[i, "Title"] = random.choice([name_conf, name_conf2])


            final_table['imgpar'] = "img.json"
            final_table['Id'] = ""
            final_table['DateBegin'] = ""
            final_table['Address'] = ""
            final_table['Город'] = "все"
            final_table['Description'] = "text_ozon.txt"
            final_table['images_folder'] = "feat"
            final_table['count_img'] = "1"
            final_table['first_img'] = "img_first_pk_ozon.json"
            final_table['Price'] = row["Сумма"]
            final_table['ImageUrls'] = ""
            final_table['Brand'] = "Другой"
            final_table['Type'] = "Игровой | Офисный"


            # Название модели (для объединения в одну карточку)* - использовать для объединения конфигов по типу кейса.

            # Название модели (для объединения в одну карточку)*

            if i % 100 == 0:
                print(f"\r  _ {i} номер строки обаботки цены {i} full_art {full_art}")
            
                # sys.stdout.write(f"\r  _ {i} номер строки обаботки цены {i} full_art {full_art}                                             \r  ")
                # sys.stdout.flush()

            # if i == 0 and i == 7611:
            #     print(f"{i} full_art {full_art} full_art_hash {full_art_hash}")

        print("" )
        
        

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


        # final_table.to_csv(f"csv/{name_df}_fin_table.csv", index=False, encoding="utf-8" )
        
        # допустим final_table уже готов
        df_final = final_table.copy()

        # 1) безопасно очистим inf/NaN и приведём к native
        # df_final = df_final.replace([np.inf, -np.inf], np.nan)
        df_final = df_final.fillna("")          # безопасно для NaN
        df_final = df_final.applymap(lambda x: x.item() if hasattr(x, "item") else x)
        df_final = df_final.reset_index(drop=True)
        
        
        
        # 2) логируем — что реально будем отправлять
        print(">>> Upload debug: rows count =", len(df_final))
        print(">>> columns count =", len(df_final.columns))
        print(">>> sample first 5 rows:\n", df_final.head(5).to_dict(orient='records'))

        # 3) Формируем rows: заголовок + все строки
        rows = [df_final.columns.tolist()] + df_final.astype(str).values.tolist()

        # 4) Ещё быстрая проверка: есть ли хотя бы одна непустая ячейка в каждой строке
        non_empty_per_row = [any(cell.strip() for cell in r) for r in rows[1:]]
        print(">>> non-empty rows count:", sum(non_empty_per_row), "of", len(non_empty_per_row))

        # 5) Очистить лист и записать — используем явный A1 диапазон
        worksheet.clear()
        time.sleep(0.5)  # короткая пауза, чтобы google успел обработать clear

        # Если есть хотя бы 1 непустая строка — записываем весь блок
        if len(df_final) > 0 and sum(non_empty_per_row) > 0:
            # Убедимся, что rows содержит корректную вложенность list[list[str]]
            worksheet.update('A1', rows, value_input_option="USER_ENTERED")
            print("Таблица успешно загружена (включая строки).")
        else:
            # Если строки есть, но все пустые — всё равно запишем, но добавим маркеры
            if len(df_final) > 0:
                # добавим индикатор в первую колонку каждой строки, чтобы гугл создал строки
                rows_with_marker = [rows[0]] + [[("!empty!" if i == 0 else "")] + r[1:] for i, r in enumerate(rows[1:], start=1)]
                worksheet.update('A1', rows_with_marker, value_input_option="USER_ENTERED")
                print("Записаны строки, которые были полностью пустыми (поставлен маркер в первой колонке).")
            else:
                print("DataFrame пуст — записаны только заголовки (ожидаемо).")
    
        print(f"{name_df}_fin_table создан. для проверки файл называется csv/{name_df}_fin_table.csv в папке csv")
        print("--------------------------------------------------------")
        print(f"|  для проверки файл называется csv/{name_df}_fin_table.csv       |")
        print("_______________________________________________________")
        print("")
        print("")

    # create_table(df_add_out,"add")
    create_table(df_blu_out,"blue")
    # create_table(df_del_out,"dell")










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


