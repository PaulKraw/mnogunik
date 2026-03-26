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

# import imgunik as img
# import textfun as txt
# import statfun as stt
# from klass import ClientParams


start_time = time.time()

ROOT_DIR = 'D:/proj/'

def delete_columns(file_path, columns_to_delete, output_suffix='_delcol.csv'):
    # Чтение файла
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Произошла ошибка при считывании файла: {e}")
        return
    
    # Удаление указанных столбцов
    df = df.drop(columns=[col for col in columns_to_delete if col in df.columns])

    # Формирование нового имени файла
    base_name, ext = os.path.splitext(file_path)
    output_file_path = base_name + output_suffix

    # Сохранение нового DataFrame в новый файл
    df.to_csv(output_file_path, index=False)
    print(f"Файл сохранен: {output_file_path}")

def read_columns_to_delete(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        columns_to_delete = [line.strip() for line in file if line.strip()]
    return columns_to_delete

def merge_csv_files(file_paths, output_file):
    # Список для хранения всех DataFrame
    dfs = []
    
    # Чтение каждого файла и добавление его в список dfs
    for file_path in file_paths:
        try:
            df = pd.read_csv(file_path)
            dfs.append(df)
        except Exception as e:
            print(f"Произошла ошибка при считывании файла {file_path}: {e}")
            return
    
    # Объединение всех DataFrame в один
    merged_df = pd.concat(dfs, ignore_index=True)
    
    # Сохранение объединенного DataFrame в новый файл
    try:
        merged_df.to_csv(output_file, index=False)
        print(f"Файл сохранен: {output_file}")
    except Exception as e:
        print(f"Произошла ошибка при сохранении файла: {e}")

name = "dezi"


del_col_path = f"{ROOT_DIR}/{name}/var/del_col_date.txt"

file1 = f"{ROOT_DIR}/{name}/dezinf1.csv"
file2 = f"{ROOT_DIR}/{name}/dezinf2.csv"
file3 = f"{ROOT_DIR}/{name}/dezinf3.csv"

# Чтение названий столбцов для удаления из файла
columns_to_delete = read_columns_to_delete(del_col_path)

# Удаление указанных столбцов из DataFrame
delete_columns(file1, columns_to_delete)
delete_columns(file2, columns_to_delete)
delete_columns(file3, columns_to_delete)


file1 = f"{ROOT_DIR}/{name}/dezinf1_delcol.csv"
file2 = f"{ROOT_DIR}/{name}/dezinf2_delcol.csv"
file3 = f"{ROOT_DIR}/{name}/dezinf3_delcol.csv"
output_file = f"{ROOT_DIR}/{name}/merged_dezinf.csv"

file_paths = [file1, file2, file3]

merge_csv_files(file_paths, output_file)

# Формирование пути для сохранения нового прайс-листа
# output_file_path = f"{ROOT_DIR}/{name}/{name}_statdate.csv"



# Конец выполнения программы
end_time = time.time()

# Время выполнения программы
execution_time = end_time - start_time
print(f"Время выполнения программы: {execution_time:.2f} секунд")
# Текущее время
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Текущее время: {current_time}")

if __name__ == "__main__":
    print("Конец.")
else:
    print("my_module.py has been imported.")

