 # -*- coding: utf-8 -*-

import sys
import csv
import gc, pandas as pd
from PIL import Image, ImageDraw, ImageFilter, ImageEnhance, ImageFont
import random
import itertools
import os
import string
# import datetime
from datetime import datetime, timedelta, time
from collections import defaultdict
import re
import chardet
import math
import ast
from dateutil import parser, tz
import json
from klass import ClientParams

import imgunik as img
import time as systime  # это не ломает твой datetime.time

# import importlib
import importlib.util

from config import ROOT_DIR_OUT, ROOT_DIR, ROOT_URL_OUT, nout

import hashlib
from functools import lru_cache

import shutil
from collections import OrderedDict
from types import ModuleType
from typing import Callable, Tuple
# create_and_perebor(cl, res_ads) #текст

# create_id(cl,res_id) #id

# create_adres(cl,res_adres) #adres

# create_title_list(cl, res_title)

# create_date_list(cl, res_date)

# create_pricecol_list(cl,res_price)

# ROOT_DIR = 'C:/proj'
# ROOT_DIR_OUT = 'C:/proj/outfile/'

# ---- 1) Кэшируем JSON по (path, mtime) ----
_json_cache = {}

def load_json_cached(path: str):
    path = os.path.abspath(path)
    if not os.path.exists(path):
        return None
    mtime = os.path.getmtime(path)
    key = (path, mtime)
    if key in _json_cache:
        return _json_cache[key]
    # вычищаем старые версии этого же файла
    for k in list(_json_cache.keys()):
        if k[0] == path:
            _json_cache.pop(k, None)
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    _json_cache[key] = data
    return data
def print_log(msg):
    with open("log.txt", "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    if nout:
        print(msg)
# ---- 2) Безопасный геттер из строки df для itertuples/iterrows ----
def safe_get(row, col_name: str, df=None):
    if hasattr(row, "_fields"):  # itertuples
        try:
            return getattr(row, col_name)
        except AttributeError:
            if df is not None and col_name in df.columns:
                idx = df.columns.get_loc(col_name)
                return row[idx]
            return None
    try:  # iterrows
        return row[col_name]
    except Exception:
        return None

# ---- 3) Загрузчик скриптов (LRU + авто-обновление по mtime) ----
class ScriptLoader:
    def __init__(self, maxsize: int = 256, func_name: str = "execute_task"):
        self.maxsize = maxsize
        self.func_name = func_name
        self._cache: "OrderedDict[tuple[str,float], callable]" = OrderedDict()

    def _key(self, abs_path: str):
        st = os.stat(abs_path)
        return (abs_path, st.st_mtime)

    def _evict(self):
        while len(self._cache) > self.maxsize:
            self._cache.popitem(last=False)

    def _load(self, abs_path: str):
        mod_name = "dynmod_" + hashlib.md5(abs_path.encode("utf-8")).hexdigest()
        spec = importlib.util.spec_from_file_location(mod_name, abs_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load spec for {abs_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore
        func = getattr(module, self.func_name, None)
        if not callable(func):
            raise AttributeError(f"{abs_path} has no callable '{self.func_name}'")
        return func

    def get(self, script_path: str):
        abs_path = os.path.abspath(script_path)
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Script not found: {abs_path}")
        key = self._key(abs_path)
        if key in self._cache:
            func = self._cache.pop(key)
            self._cache[key] = func
            return func
        # удалить старые версии по тому же пути (другой mtime)
        for k in list(self._cache.keys()):
            if k[0] == abs_path:
                self._cache.pop(k, None)
        func = self._load(abs_path)
        self._cache[key] = func
        self._evict()
        return func
    

def load_imgparam_from_first_img(ROOT_DIR, cl, row, df):
    # если колонки нет — возвращаем None, строку можно пропустить
    if df is not None and 'first_img' not in df.columns:
        return None
    val = safe_get(row, 'first_img', df)
    # пустые/NaN — тоже None
    if val is None or (isinstance(val, float) and pd.isna(val)) or (isinstance(val, str) and not val.strip()):
        return None
    path = os.path.join(ROOT_DIR, cl.name, "var", "img", str(val))
    if not os.path.exists(path):
        return None
    return load_json_cached(path)  # dict или None



def read_city_distribution(file_path, target_count):
    city_distribution = defaultdict(int)
    total_ratio = 0

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            city = row['Город'].strip()  # Удаляем лишние пробелы
            if city:  # Проверка на пустые строки
                ratio = int(row['число'])
                city_distribution[city] += ratio  # Используем defaultdict для суммирования
                total_ratio += ratio

    if total_ratio == 0:
        raise ValueError("Общая доля равна нулю. Проверьте значения 'число' в файле распределения городов.")

    # Расчет количества строк для каждого города
    for city in city_distribution:
        city_distribution[city] = round((city_distribution[city] / total_ratio) * target_count)

    total_assigned = sum(city_distribution.values())
    difference = target_count - total_assigned
    if difference != 0:
        first_city = next(iter(city_distribution))
        city_distribution[first_city] += difference


    return dict(city_distribution)  # Преобразуем обратно в обычный словарь, если нужно

def duplicate_rows(df, target_count, city_distribution):
    print(df.columns)
    
    # Проверка наличия всех необходимых городов в прайс-листе
    unique_cities_in_price = df['Город'].unique()
    
    # Создание нового DataFrame с дублированными строками
    new_df = pd.DataFrame()

    # Дублирование строк для городов, которые есть в прайс-листе
    for city, count in city_distribution.items():
        if city in unique_cities_in_price:
            city_df = df[df['Город'] == city]
            # Дублирование строк для данного города
            repeated_df = pd.concat([city_df] * (count // len(city_df) + 1), ignore_index=True)
            new_df = pd.concat([new_df, repeated_df.iloc[:count]], ignore_index=True)

    # Проверка, нужно ли заполнять оставшиеся строки
    remaining_count = target_count - len(new_df)
    if remaining_count > 0:
        # Проверка наличия города "все" для заполнения оставшихся строк
        all_cities_df = df[df['Город'] == 'все']
        
        # Если есть строки "все", то используем их для заполнения оставшихся строк
        if not all_cities_df.empty:
            repeated_df = pd.concat([all_cities_df] * (remaining_count // len(all_cities_df) + 1), ignore_index=True)
            repeated_df = repeated_df.iloc[:remaining_count]
        else:
            raise ValueError("Нет данных для города 'все' для заполнения оставшихся строк.")

        # Вставка недостающих городов в дублированные строки
        missing_cities = [city for city in city_distribution if city not in unique_cities_in_price]
        missing_count = {city: city_distribution[city] for city in missing_cities}
        
        index = 0
        for city, count in missing_count.items():
            for _ in range(count):
                if index < len(repeated_df):
                    repeated_df.at[index, 'Город'] = city
                    index += 1
                else:
                    break
        
        new_df = pd.concat([new_df, repeated_df], ignore_index=True)

    return new_df
def duplicate_rows_robust(
    df,
    target_count,
    city_distribution,
    city_col='Город',
    all_key='все',
    shuffle=False
):
    """
    Новая логика:

    1) Если в таблице есть колонка 'countown' и в строке стоит число > 0 —
       ДУБЛИРУЕМ эту строку ровно countown раз. Эти строки фиксированы и дальше
       ни во что не «добираются» и не участвуют в пропорциях.

    2) Строки без значения в 'countown' (нет столбца / NaN / 0):
       - Если city == 'все' → попадают в ПУЛ ДЛЯ РАСПРЕДЕЛЕНИЯ (источник копий).
       - Если city != 'все' → НЕ трогаем (оставляем как есть, без дублирования).

    3) Распределение city_distribution {город -> число} применяем ТОЛЬКО к пулу 'все'
       (пустой/нулевой countown). Для каждого города создаём нужное кол-во копий
       из пула 'все' и проставляем Город = целевой город. Если пула нет — ничего не делаем.

    4) Если в итоге строк меньше target_count и нет строк, которые можно добрать
       (т.е. нет пула 'все' без countown) — оставляем как есть (НЕ падаем).

    5) Ничего не обрезаем по target_count (по твоей просьбе). Порядок сборки:
       [фиксированные по countown] + [нетронутые НЕ-'все'] + [распределённые из 'все'].

    Примечание: city_distribution ключи используются как итоговые значения в колонке 'Город'
    (регистр/написание — как в ключах).
    """
    import pandas as pd
    import numpy as np

    if city_col not in df.columns:
        raise ValueError(f"Нет колонки '{city_col}' в DataFrame")

    # Копия и тех.колонка для нормализации города
    work = df.copy()
    work[city_col] = work[city_col].astype(str).str.strip()
    work['_city_norm'] = work[city_col].str.lower().str.strip()

    # Нормализуем ключ для 'все'
    all_norm = str(all_key).strip().lower()

    # Подготовка countown (может отсутствовать)
    has_countown = 'countown' in work.columns
    if has_countown:
        # Преобразуем к числу, невалидные -> NaN
        work['_countown_num'] = pd.to_numeric(work['countown'], errors='coerce')
    else:
        work['_countown_num'] = np.nan  # всё считается "без countown"

    # ---- 1) ФИКСИРОВАННЫЕ строки: countown > 0 ----
    fixed_mask = work['_countown_num'].fillna(0) > 0
    fixed_rows = work[fixed_mask].drop(columns=['_city_norm'])
    fixed_list = []

    if not fixed_rows.empty:
        # Дублируем каждую строку ровно countown раз
        reps = fixed_rows['_countown_num'].astype(int).tolist()
        # Для ускорения: повторить индексы согласно reps
        repeated = fixed_rows.loc[fixed_rows.index.repeat(reps)].copy()
        fixed_list.append(repeated)

    # ---- 2) НЕтронутые НЕ-'все' без countown ----
    flex_mask = ~fixed_mask  # строки, где countown нет/0/NaN
    # из них берем те, у кого город != 'все'
    untouched_mask = flex_mask & (work['_city_norm'] != all_norm)
    untouched_rows = work[untouched_mask].drop(columns=['_city_norm'])

    # ---- 3) Пул для распределения: только 'все' без countown ----
    pool_mask = flex_mask & (work['_city_norm'] == all_norm)
    pool_rows = work[pool_mask].drop(columns=['_city_norm'])

    # ---- 4) Распределение по городам (только из пула 'все') ----
    # Нормализуем входной словарь (не меняя регистр ключей для итоговой подстановки)
    # При этом фильтруем нулевые/отрицательные
    norm_dist_items = [(str(k), int(v)) for k, v in city_distribution.items() if int(v) > 0]
    dist_clones = []

    if not pool_rows.empty and norm_dist_items:
        # Сформируем один объединённый пул (может быть из нескольких базовых строк 'все')
        # Далее будем повторять этот пул и подрезать на нужное количество.
        for city_target, need_count in norm_dist_items:
            if need_count <= 0:
                continue
            # повторим пул, чтобы хватило записей
            reps = (need_count // len(pool_rows)) + 1
            repeated_pool = pd.concat([pool_rows] * reps, ignore_index=True).iloc[:need_count].copy()
            # проставим целевой город (как в ключе)
            repeated_pool[city_col] = city_target
            dist_clones.append(repeated_pool)

    # ---- 5) Сборка результата ----
    parts = []
    if fixed_list:
        parts.append(pd.concat(fixed_list, ignore_index=True))
    if not untouched_rows.empty:
        parts.append(untouched_rows.reset_index(drop=True))
    if dist_clones:
        parts.append(pd.concat(dist_clones, ignore_index=True))

    if parts:
        out = pd.concat(parts, ignore_index=True)
    else:
        # ничего не собралось — вернём пустую таблицу с исходными колонками
        out = df.copy().iloc[0:0]

    # Тех.колонки прибрали выше; оставим исходные колонки как есть (включая countown, если была)
    # Порядок колонок: как в исходном df
    out = out[df.columns.intersection(out.columns).tolist()] \
          .reindex(columns=df.columns, fill_value=None)

    # ---- 6) Перемешивание по желанию ----
    if shuffle and len(out) > 1:
        out = out.sample(frac=1, random_state=42).reset_index(drop=True)

    # Ничего НЕ обрезаем по target_count (по твоей просьбе).
    return out

def write_city_list_csv(params, ROOT_DIR, shuffle=False, encoding="utf-8-sig", logger=print):
    """
    Создаёт CSV с одной колонкой 'Город' на основе файла распределения городов (params.k_gorod).
    - Берёт вход как у твоей duplicate_rows: txt.read_city_distribution(file, params.num_ads)
    - Итоговый файл кладёт туда же, куда падает финальный CSV: ROOT_DIR/<client>/
    - Имя: cities_{name}_{name_csv}_{date_f}_{num_ads}.csv
    """
    # 1) Путь к файлу распределения городов (как у тебя в коде)
    city_distribution_file = f"{ROOT_DIR}/{params.name}/{params.k_gorod}"
    if not os.path.isfile(city_distribution_file):
        raise FileNotFoundError(f"Не найден файл распределения городов: {city_distribution_file}")

    # 2) Читаем распределение (твой метод)
    city_distribution = read_city_distribution(str(city_distribution_file), params.num_ads)
    # txt.read_city_distribution обычно даёт dict: {"Москва": 3, "Тверь": 2, ...}
    # на всякий случай поддержим и список пар
    if hasattr(city_distribution, "items"):
        items = list(city_distribution.items())
    else:
        items = list(city_distribution)  # [(city, n), ...]

    # 3) Собираем строки
    rows = []
    for city, n in items:
        try:
            n = int(n)
        except Exception:
            continue
        if n <= 0:
            continue
        rows.extend([str(city)] * n)

    # 4) Перемешивание по желанию
    if shuffle and rows:
        import random
        random.Random(42).shuffle(rows)

    # 5) Куда сохраняем (туда же, где твой финальный файл)
    out_dir = f"{ROOT_DIR}/{params.name}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/cities_{params.name}_{params.name_csv}_{params.date_f}_{params.num_ads}.csv"

    # 6) Пишем CSV дружественно к Excel
    # через pandas — покороче и надёжно по кодировке
    pd.DataFrame({"Город": rows}).to_csv(out_path, index=False, encoding=encoding)

    if logger:
        logger(f"✅ Файл городов создан: {out_path} (строк: {len(rows)})")
    return out_path


def create_and_process_adres(cl, df):
    
    res_adres = []
    adres_gorod = f"{ROOT_DIR}/{cl.name}/var/adres/adres_gorod.csv"

    if not os.path.exists(adres_gorod):
        adres_gorod = "vars/adres_gorod.csv"

    city = df['Город'].tolist()
    # print("Длина списка city:", len(city))

    addresses_with_full_address = read_addresses_with_full_address_file(adres_gorod)
    # for i, city in enumerate(city):
    #     if city in addresses_with_full_address:
    #         random_address = random.choice(addresses_with_full_address[city])
    #         res_adres.append(random_address)
    #     else:
    #         res_adres.append(city)
    
    # idlist = create_id(cl)
    # print("Длина списка res_adres после заполнения:", len(res_adres))

    for index, row in df.iterrows():
        # res_date = []
        if pd.notna(city[index]):
                # Преобразуем в строку и удаляем пробелы
                addr_str = str(city[index]).strip()
                # Если длина больше 2, считаем, что адрес уже есть — пропускаем
                if len(addr_str) > 2:
                    continue
    # else:
        # cityone = row.get('Город', '')
        if city[index] in addresses_with_full_address:
            random_address = random.choice(addresses_with_full_address[city[index]])
            df.at[index, 'Address'] = random_address
        else:
            df.at[index, 'Address'] = city[index]


        # df.at[index, 'Address'] = res_adres[index]

    return df



# Функция для считывания данных из CSV
def read_params_from_csv(file_path):
    params_list = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if row['run'] == '1':
                # print(row['file_price'])
                json_path = f"{ROOT_DIR}/{row['name']}/var/img/img.json"

                with open(json_path, 'r', encoding='utf-8') as jsonfile:
                    imgparam = json.load(jsonfile)  

                periods_raw = row.get('periods', '')  # безопасно получаем значение
                periods = []

                if periods_raw:
                    try:
                        periods = ast.literal_eval(periods_raw)
                    except (ValueError, SyntaxError):
                        periods = []

                params = ClientParams(
                    name=row['name'],
                    name_csv=row['name_csv'],
                    cat_wp=row['cat_wp'],

                    k_gorod=f"var/adres/{row['k_gorod']}",
                    num_ads=int(row['num_ads']),
                    date_f=row['date_f'],
                    num_days=1,
                    # num_days=int(row['num_days']),
                    # end_date=int(row['end_date']),
                    end_date=30,

                    

                    file_price=row['file_price'],
                    
                    periods="",
                    # periods=ast.literal_eval(row['periods']),

 
                    

                    # shuffle_list=row['shuffle_list'].lower() == 'true',
                    imgparam=imgparam,

                    address_to_append = row['address_to_append'],
                    
                    info_dict = {
                        'CompanyName' : row['CompanyName'],
                        'EMail' : row['EMail'],
                        'ContactMethod' : row['ContactMethod'],
                        'ContactPhone' : row['ContactPhone'],
                        'ManagerName' : row['ManagerName'],


                    }
                )
                params_list.append(params)
    return params_list

def parse_price_field(field_str):
    field_str = field_str.strip()

    # Проверка конструкции rand(min;max;step)
    match = re.match(r'rand\((\d+);(\d+);(\d+)\)', field_str)
    if match:
        start = int(match.group(1))
        end = int(match.group(2))
        step = int(match.group(3))
        possible_values = list(range(start, end + 1, step))
        value = random.choice(possible_values)
        return value

    # Если просто число, преобразуем
    try:
        value = int(field_str)
        return value
    except ValueError:
        return None  # или 0, если нужно
    

# обработка 1 значения описания : считывание ячейки описания т е его шаблона, обработка, и вставка текста.
def create_and_process_text(cl, df, ROOT_DIR):

    # Загрузка всех списков из директории в словарь
    lists_directory = f"{ROOT_DIR}/{cl.name}/var/lists"
    print(lists_directory)


    lists_dict = load_lists_from_directory(lists_directory)  #ultrafps сборка пк
    
    for index, row in df.iterrows():
        # print(df.at[index, 'Price'])
        if 'Price' in row and pd.notna(row['Price']):

            df.at[index, 'Price'] = parse_price_field(row['Price'])

        if 'temp_Description' in row and (pd.isna(row['temp_Description']) or row['temp_Description'] == '') and pd.notna(row['Description']):
            # print('hui')
            continue

        if 'temp_Description' in row and pd.notna(row['temp_Description']):
            input_file_path = f"{ROOT_DIR}/{cl.name}/var/text/{row['temp_Description']}"
        else:
            input_file_path = f"{ROOT_DIR}/{cl.name}/var/text/{row['Description']}"


        with open(input_file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        if index == 3:
            print_log(f"текстовый шаблон : {input_file_path} ")
            print_log(f"текстовый шаблон : {input_file_path} ")
            print_log(f"текстовый шаблон : {input_file_path} ")

        lines = replace_vartext(lines, cl.name)
        

                               
        
        combined_text = ""
        begp = 0
        endp = 0

        for j in range(len(lines)):
            text = lines[j]
            trimmed_text = text.strip()
            if "$perebor" in trimmed_text and any(c.isalnum() for c in trimmed_text):
                begp, endp = j + 1, 0

            if "$/perebor" in trimmed_text and any(c.isalnum() for c in trimmed_text):
                endp = j
                perebor = lines[begp:endp]
                random.shuffle(perebor)
                lines[begp:endp] = perebor

        combined_text = "".join(lines).replace("\n", " ")
        combined_text = combined_text.replace("$/perebor", "")
        combined_text = combined_text.replace("$perebor", "")

        # вставляем текст из файла
        # combined_text = replace_vartext(combined_text, cl.name)

        # вставляем подстановку гипотез 
        # с созданием ариткула гипотезы в таблице
        combined_text = replace_gipotez(combined_text, row, cl.name)

        # print({getattr(row, 'articul')})
        combined_text = replace_synonyms(combined_text)
        # print(f"после replace_synonyms{getattr(row, 'articul')}")

        # Обработка $pastetable(dopprice) и других подобных шаблонов
        combined_text = re.sub(r'\$pastetable\((.*?)\)', lambda match: str(row.get(match.group(1), '')), combined_text)


        

        combined_text = replace_vars(combined_text, generate_rand_unique_objects(cl, input_file_path))

        
        combined_text = re.sub(r'\{(.*?)\}', replace_template, combined_text)
        # combined_text = re.sub(r'gen_int\((\d+),(\d+)\)', replace_template_rand, combined_text)

        combined_text = re.sub(r'gen_int\(\d+,\d+(?:,\d+)?\)', replace_gen_int_with_step, combined_text)


        
        def replace_with_random_line(match):
            list_name = match.group(1) + ".txt"
            if list_name in lists_dict:
                # Если файл уже загружен, выбираем случайную строку
                return random.choice(lists_dict[list_name]).strip()
            else:
                # Если файл не найден, возвращаем пустую строку или можно обработать это иначе
                return ''
            
        
        # Проверка на наличие подстановок $pastevarlists(...) в тексте
        if re.search(r'\$pastevarlists\((.*?)\)', combined_text) and not lists_dict:
            print("Ошибка: В тексте найдены подстановки для списков, но папка со списками пуста!")
            sys.exit(1)  # Завершение программы с кодом 1 (ошибка)
        # Замена по шаблону $pastevarlists(...)


        combined_text = re.sub(r'\$pastevarlists\((.*?)\)', replace_with_random_line, combined_text)



        art_match = re.search(r'<art>(.*?)</art>', combined_text)
        if art_match:
            art_text = art_match.group(1).strip()
            df.at[index, 'art-gip'] = art_text
            combined_text = re.sub(r'<art>.*?</art>', '', combined_text).strip()


        # Обновление столбца Description в DataFrame
        df.at[index, 'Description'] = combined_text

        if len(combined_text)>7500:
            print(f"Длина строки Description больше 7500 символов. : строка /{combined_text}/")
            while True:
                user_input = input("Продолжить выполнение программы? (да/нет): ").strip().lower()
                if user_input == 'да':
                    print("Продолжаем выполнение программы.")
                    break
                elif user_input == 'нет':
                    print("Завершаем выполнение программы.")
                    exit()
                else:
                    print("Неправильный ввод. Пожалуйста, введите 'да' или 'нет'.")


    # Дублирование значений из cl.info_dict
    # for column, value in cl.info_dict.items():
    #     df[column] = value   

    # # Проверка ContactPhone и загрузка телефонов из файла
    # if cl.info_dict['ContactPhone'].endswith('.txt'):
    #     phone_file_path = cl.info_dict['ContactPhone']
    #     phone_numbers = read_phone_numbers_from_file(f"{ROOT_DIR}/{cl.name}/var/{phone_file_path}")
        
    #     default_phone = phone_numbers.get('по умолчанию', None)
        
    #     for index, row in df.iterrows():
    #         city = row['Город']
    #         phone = phone_numbers.get(city, default_phone)
    #         df.at[index, 'ContactPhone'] = phone



    return df

   

# обработка 1 значения описания : считывание ячейки описания т е его шаблона, обработка, и вставка текста.
def create_and_process_unik_text(cl, df, ROOT_DIR):

        # Загрузка всех списков из директории в словарь
    lists_directory = f"{ROOT_DIR}/{cl.name}/var/lists"
    print(lists_directory)


    lists_dict = load_lists_from_directory(lists_directory)  #ultrafps сборка пк

    for index, row in df.iterrows():
        # print(df.at[index, 'Price'])
        # df.at[index, 'Price'] = parse_price_field(row['Price'])

        # if 'temp_unik_Description' in row and (pd.isna(row['temp_unik_Description']) or row['temp_unik_Description'] == '') and pd.notna(row['new_param']):
        if 'temp_unik_Description' in row and (pd.isna(row['temp_unik_Description']) or row['temp_unik_Description'] == ''):
            continue
        # print(f"{row['temp_unik_Description']} - {row['param_unik']}")
        if 'temp_unik_Description' in row and pd.notna(row['temp_unik_Description']):
            input_file_path = f"{ROOT_DIR}/{cl.name}/var/text/{row['temp_unik_Description']}"
        else:
            input_file_path = f"{ROOT_DIR}/{cl.name}/var/text/{row['param_unik']}"


        with open(input_file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        lines = replace_vartext(lines, cl.name)
        

                               
        
        combined_text = ""
        begp = 0
        endp = 0

        for j in range(len(lines)):
            text = lines[j]
            trimmed_text = text.strip()
            if "$perebor" in trimmed_text and any(c.isalnum() for c in trimmed_text):
                begp, endp = j + 1, 0

            if "$/perebor" in trimmed_text and any(c.isalnum() for c in trimmed_text):
                endp = j
                perebor = lines[begp:endp]
                random.shuffle(perebor)
                lines[begp:endp] = perebor

        combined_text = "".join(lines).replace("\n", " ")
        combined_text = combined_text.replace("$/perebor", "")
        combined_text = combined_text.replace("$perebor", "")

        # вставляем текст из файла
        # combined_text = replace_vartext(combined_text, cl.name)

        # вставляем подстановку гипотез 
        # с созданием ариткула гипотезы в таблице
        combined_text = replace_gipotez(combined_text, row, cl.name)

        # print({getattr(row, 'articul')})
        combined_text = replace_synonyms(combined_text)
        # print(f"после replace_synonyms{getattr(row, 'articul')}")

        # Обработка $pastetable(dopprice) и других подобных шаблонов
        combined_text = re.sub(r'\$pastetable\((.*?)\)', lambda match: str(row.get(match.group(1), '')), combined_text)


        

        combined_text = replace_vars(combined_text, generate_rand_unique_objects(cl, input_file_path))

        
        combined_text = re.sub(r'\{(.*?)\}', replace_template, combined_text)
        # combined_text = re.sub(r'gen_int\((\d+),(\d+)\)', replace_template_rand, combined_text)

        combined_text = re.sub(r'gen_int\(\d+,\d+(?:,\d+)?\)', replace_gen_int_with_step, combined_text)


        
        def replace_with_random_line(match):
            list_name = match.group(1) + ".txt"
            if list_name in lists_dict:
                # Если файл уже загружен, выбираем случайную строку
                return random.choice(lists_dict[list_name]).strip()
            else:
                # Если файл не найден, возвращаем пустую строку или можно обработать это иначе
                return ''
            
        
        # Проверка на наличие подстановок $pastevarlists(...) в тексте
        if re.search(r'\$pastevarlists\((.*?)\)', combined_text) and not lists_dict:
            print("Ошибка: В тексте найдены подстановки для списков, но папка со списками пуста!")
            sys.exit(1)  # Завершение программы с кодом 1 (ошибка)
        # Замена по шаблону $pastevarlists(...)


        combined_text = re.sub(r'\$pastevarlists\((.*?)\)', replace_with_random_line, combined_text)



        art_match = re.search(r'<art>(.*?)</art>', combined_text)
        if art_match:
            art_text = art_match.group(1).strip()
            df.at[index, 'art-gip'] = art_text
            combined_text = re.sub(r'<art>.*?</art>', '', combined_text).strip()


        # print(f"{row['temp_unik_Description']} {row['AvitoId']}  - {combined_text}")


        # Обновление столбца Description в DataFrame
        df.at[index, 'param_unik'] = combined_text

        if len(combined_text)>3500:
            print(f"Длина строки с уникальными значениями более 3500 символов. : строка /{combined_text}/")
            while True:
                user_input = input("Продолжить выполнение программы? (да/нет): ").strip().lower()
                if user_input == 'да':
                    print("Продолжаем выполнение программы.")
                    break
                elif user_input == 'нет':
                    print("Завершаем выполнение программы.")
                    exit()
                else:
                    print("Неправильный ввод. Пожалуйста, введите 'да' или 'нет'.")



    return df


# для загрузки списков в словарь
def load_lists_from_directory(directory_path):
    lists_dict = {}
    # return lists_dict 
    # Загрузка всех txt файлов в директории
    if os.path.exists(directory_path):
        for filename in os.listdir(directory_path):
            if filename.endswith(".txt"):
                file_path = os.path.join(directory_path, filename)
                print(file_path)
                with open(file_path, 'r', encoding='utf-8') as file:
                    lists_dict[filename] = file.readlines()

    # Если ни один файл не был загружен, выдать ошибку
    if not lists_dict:
        print("Ошибка: Папка со списками пуста!")
        # sys.exit(1)  # Завершение программы с кодом 1 (ошибка)

    return lists_dict


def read_phone_numbers_from_file(file_path):
    phone_numbers = {}
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            parts = line.strip().split(',')
            city = parts[0]
            phone = parts[1]
            phone_numbers[city] = phone
    return phone_numbers

    
# создание массива строк текста из шаблона#
def create_and_perebor(cl):
    res_ads = []
    input_file_path = f"vars/{cl.orig_t}"
    max_combinations = cl.num_ads
    
    # Чтение содержимого файла и подсчет количества строк
    with open(input_file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        num_lines = len(lines)

    for i in range(max_combinations):
        begp = 0
        endp = 0
        combined_text = ""
        for j in range(num_lines):

            text = lines[j]
            trimmed_text = text.strip() 
            if "$perebor" in trimmed_text and any(c.isalnum() for c in trimmed_text):
                begp, endp = j + 1, 0

            if "$/perebor" in trimmed_text and any(c.isalnum() for c in trimmed_text):
                endp = j
                perebor = []
                combined_text = []
                for k in range(endp - begp):
                    perebor.append(lines[begp + k])
                random.shuffle(perebor)
                for k in range(len(perebor)):
                    lines[begp + k] = perebor[k]

        combined_text = "".join(lines)
        combined_text = combined_text.replace("\n", " ")

        # combined_text = replace_synonyms(combined_text)

        res_ads.append(combined_text)


    unique_objects = generate_rand_unique_objects(cl)
 
 
    for i in range(len(res_ads)):
        res_ads[i] = res_ads[i].replace("$/perebor", "")
        res_ads[i] = res_ads[i].replace("$perebor", "")
        res_ads[i] = res_ads[i].replace("$sin", "")
        res_ads[i] = re.sub(r'\{(.*?)\}', replace_template, res_ads[i])
        res_ads[i] = re.sub(r'gen_int\((\d+),(\d+)\)', replace_template_rand, res_ads[i])

        # print(f"{cl.name_csv}/ текст для объявления № {i+1}:")

        res_ads[i] = replace_synonyms(res_ads[i])

        res_ads[i] = replace_vars(res_ads[i], unique_objects[i])


    output_file = 'output_file\\txt\\output_text.txt'  # Имя файла для записи
    with open(output_file, 'w', encoding='utf-8') as file:
         for line in res_ads:
            file.write(line + "\n")  # Запишите строку в файл
            


    
    
    return res_ads


def replace_synonyms(text):
    # Функция для выбора случайного синонима из блока
    def process_block(inner_text):
        # Убираем внешние $sin{ и $sin}
        inner_text = inner_text[5:-5].strip()
        # Разделяем на синонимы и выбираем случайный
        synonyms = inner_text.split('||')
        synonyms = [syn.strip() for syn in synonyms if syn.strip()]  # Убираем лишние пробелы
        return random.choice(synonyms)
    
    def process_text(text):
        # Функция для обработки вложенных конструкций
        def replace_match(match):
            full_match = match.group(0)
            inner_text = full_match[5:-5]
            return process_block(full_match)

        # Регулярное выражение для нахождения конструкций
        pattern = r'\$sin\{[^{}]*\s*\$sin\}'
        while re.search(pattern, text, flags=re.DOTALL):
            text = re.sub(pattern, replace_match, text, flags=re.DOTALL)
        
        return text
    
    # Обрабатываем текст
    processed_text = process_text(text)
    
    # Обрабатываем оставшиеся конструкции, если таковые имеются
    final_pattern = r'\$sin\{(.*?)\s*\$sin\}'
    while re.search(final_pattern, processed_text, flags=re.DOTALL):
        processed_text = re.sub(final_pattern, lambda match: process_block(match.group(0)), processed_text, flags=re.DOTALL)
    
    return processed_text

def replace_vartext(lines, cl_name):
    # Путь к текстам
    input_file_path = os.path.join(ROOT_DIR, cl_name, "var", "text")
    new_lines = []

    pattern = re.compile(r'pastetxt\((.*?)\)')

    for line in lines:
        matches = pattern.findall(line)

        if not matches:
            new_lines.append(line)
            continue

        # Обрабатываем все вхождения вида pastetxt(...)
        for match in matches:
            file_name = f"{match}.txt"
            full_path = os.path.join(input_file_path, file_name)

            if os.path.isfile(full_path):
                with open(full_path, encoding="utf-8") as f:
                    file_content = f.read()

                    if file_content and not file_content.endswith(('\n', '\r')):
                        file_content += '\n'
            else:
                file_content = ''  # Если файла нет — пусто

            line = line.replace(f"pastetxt({match})", file_content)

        new_lines.extend(line.splitlines(keepends=True))
        # new_lines.append(line)

    return new_lines

def replace_gipotez(combined_text: str, row: dict, cl_name: str) -> str:
    """
    Заменяет в combined_text вызовы pastegipotez(...) на данные из CSV-файла по артикулу getattr(row, 'articul').
    
    Аргументы:
    - combined_text: текст с вызовами pastegipotez(...)
    - row: словарь с полем 'articul' для выбора нужных данных
    - cl_name: имя папки, где искать CSV
    
    Возвращает:
    - текст с заменёнными вставками или выбрасывает исключение с ошибкой
    """
    
    # для поиска всех вхождений pastegipotez(что-то)
    pattern = re.compile(r'pastegipotez\((.*?)\)')
    matches = pattern.findall(combined_text)

    if not matches:
        # Если в тексте нет таких вызовов — возвращаем исходный текст без изменений
        return combined_text

    articul = row.get('articul')
    if not articul:
        raise ValueError("В row нет ключа 'articul'")
    


    # Путь к файлу CSV
    input_file_path = os.path.join(ROOT_DIR, cl_name, "var", "text", f"gipotez.csv")
    
    if not os.path.isfile(input_file_path):
        raise FileNotFoundError(f"Файл {input_file_path} не найден")
    
    # Читаем CSV
    df = pd.read_csv(input_file_path)

    # Фильтруем по артикулу
    df_filtered = df[df['articul'] == articul]
    if df_filtered.empty:
        raise ValueError(f"В файле {input_file_path} нет строк с articul={articul}")
    
    # Выбираем случайную строку из отфильтрованных
    selected_row = df_filtered.sample(n=1).iloc[0]


    # Функция замены для re.sub
    def replacement(match_obj):
        key = match_obj.group(1).strip()
        # Если ключа нет в столбцах — возвращаем исходный текст без изменений (или можно ошибку)
        if key not in selected_row:
            # Можно логировать, что нет такого поля, либо заменить на пустую строку
            return f"[Нет поля {key}]"
        return str(selected_row[key])
    
    # Заменяем все вхождения pastegipotez(...)
    result_text = pattern.sub(replacement, combined_text)


    return result_text

def strip_html_tags(text):
    """ Функция для удаления HTML тегов из текста """
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def replace_template(match):
    # Получаем содержимое шаблона между фигурными скобками
    template = match.group(1)
    
    # Разбиваем шаблон на функцию и аргументы
    parts = template.split('(')
    function_name = parts[0]
    
    # Проверяем, является ли шаблон вызовом функции
    if len(parts) > 1:
        args = parts[1].strip(')').split(',')
        if function_name == "gencode":
            if len(args) == 1:
                return generate_random_code(int(args[0]))
        elif function_name == "genchar":
            if len(args) == 1:
                return generate_random_char_code(int(args[0]))
        elif function_name == "genhex":
            return generate_random_hex_color()
    
    # Если шаблон не является вызовом функции, вернем его без изменений
    return "{" + template + "}"

def replace_vars(text, values):
    pattern = r'\{\$var_(\d+),[^}]*\}' 

    values_list = values


    def replace(match):
        var_number = int(match.group(1)) - 1
        # print(f"values_list {values_list} ")

        return values_list[var_number]




    
    result = re.sub(pattern, replace, text)

    # print(f"result - {result}") 
    return result

def replace_template_rand(match):
    # Функция для замены шаблона gen_int(10,30) случайным числом от 10 до 30
    expression = match.group(0)  # Получаем строку вида gen_int(10,30)
    # Найдем числа в скобках
    num1, num2 = map(int, re.findall(r'\d+', expression))
    # Генерируем случайное число в указанном диапазоне
    return str(random.randint(num1, num2))


def replace_gen_int_with_step(match):
    """
    Заменяет шаблон gen_int(min, max, [step]) на случайное число из диапазона с учетом шага.
    Шаблон может быть: gen_int(10,30) или gen_int(10,30,5)
    """
    # Получаем строку вида gen_int(10,30) или gen_int(10,30,5)
    expression = match.group(0)
    # Извлекаем числа
    numbers = list(map(int, re.findall(r'\d+', expression)))

    if len(numbers) == 2:
        start, end = numbers
        step = 1
    elif len(numbers) == 3:
        start, end, step = numbers
    else:
        raise ValueError(f"Неверный формат выражения: {expression}")

    # Генерация случайного значения с шагом
    values = list(range(start, end + 1, step))
    if not values:
        raise ValueError(f"Диапазон с шагом {step} от {start} до {end} недопустим.")
    return str(random.choice(values))

#использование функции для генерации уникальных объектов
def generate_rand_unique_objects(cl, text=None):
    
    pattern = r'\{\$var_(\d+),\s+prm:\s+([\d.]+),\s+mxp:\s+([\d.]+),\s+mxm:\s+([\d.]+),\s+stp:\s+([\d.]+)\}'

    if (text==None):
        input_file_path = f"vars/{cl.orig_t}"
        max_combinations = max(cl.num_ads, 1000)
        original_object = []
        variants = []

    else:
        input_file_path = text
        # print(f"читаем текст    --    {text}")
        max_combinations = 1

    
    original_object = []
    variants = []

    with open(input_file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

        for j in range(len(lines)):
            matches = re.search(pattern, lines[j])

            if matches:
                var_number = int(matches.group(1))
                params = {
                    "prm": float(matches.group(2).replace(',', '.')),
                    "mxp": float(matches.group(3).replace(',', '.')),
                    "mxm": float(matches.group(4).replace(',', '.')),
                    "stp": float(matches.group(5).replace(',', '.'))
                }
                gen_num_array = generate_number_array(params)
                variants.append(gen_num_array)
                original_object.append(params)

    unique_objects = []
    unique_objects_new = []

    # print('Приступаю к созданию вариаций')

    for ic in range(max_combinations):
        #создание случайного параметра
        rand_combo = get_rand_product(variants, unique_objects, max_combinations)
        if ic>1:
            print(f"for ic in range(max_combinations):{ic}")
        unique_objects_new.append(rand_combo)
        # unique_objects.append(rand_combo)

    # print('Вариации созданы')
 
 
    if (text==None):
        result = unique_objects_new

    else:
        result = unique_objects_new[0]


    return result

def generate_number_array(params):
    prm = params["prm"]
    mxp = params["mxp"]
    mxm = params["mxm"]
    stp = params["stp"]

    # Генерируем массив чисел на основе параметров
    number_array = [prm]
    current_number = prm
    while current_number + stp <= prm + mxp:
        current_number += stp
        number_array.append(current_number)
    current_number = prm
    while current_number - stp >= prm - mxm:
        current_number -= stp
        number_array.append(current_number)

    return number_array

def smart_format(val):
    val = round(val, 2)
    if val == int(val):
        return str(int(val))  # убираем .0
    else:
        return f"{val:.2f}"  # оставляем до двух знаков

def get_rand_product(variants, unique_objects, max_combinations):
    chek_max = 0
    while chek_max < int(max_combinations*10000/100):
        random_combination = []
        if chek_max>1:
            print(f"if chek_max>1::{chek_max}")
        for var in variants:
            # Выбираем два случайных числа из текущего варианта
            rand_nums = random.sample(var, 2)
            # Сортируем и добавляем в результат

            min_val = smart_format(min(rand_nums))
            max_val = smart_format(max(rand_nums))

            probabilities = [0.1, 0.1, 0.2, 0.2, 0.3, 0.1] 
            formats = [
                "от {min_val} до {max_val} ",
                "от {min_val} до {max_val}",
                "{min_val} - {max_val} ",
                "{min_val} - {max_val}",
                "{min_val}-{max_val}",
                "в пределах {min_val}-{max_val}"
            ]

            chosen_format = random.choices(formats, probabilities)[0]  # Выбор случайного формата с заданными вероятностями

            random_combination.append(chosen_format.format(min_val=min_val, max_val=max_val))

            # random_combination.append(f"{min(rand_nums)} - {max(rand_nums)} ")

            # random_combination.append(f"{min(rand_nums)} - {max(rand_nums)} ")

        # if is_unique(random_combination, unique_objects, unique_params):
        #     unique_objects.append(random_combination)
        return random_combination
        # else:
        #     chek_max += 1
    print("Необходимо сгенерировать больше вариаций! Иначе рандом не работает!")
    return random_combination

def create_id(cl):
    res_id = []
    

    for i in range(1, cl.num_ads + 1):
        current_date = datetime.now()
        formatted_date = current_date.strftime("%y%m%d-%H")
        id_string = f"{cl.name_csv}_{formatted_date}-{i:03}"
        res_id.append(id_string)
    # print(res_id)
    return res_id

def create_and_process_id(cl, df):


    if 'art-gip' in df.columns:
        art_gip_list = df['art-gip'].tolist()
    else:
        art_gip_list = [''] * len(df)  # Пустой список длиной с df

    articul_list = df['articul'].tolist()
    
    if 'DateBegin' in df.columns:
        date_beg = df['DateBegin'].tolist()
    else:
        date_beg = ['' for _ in articul_list]  # или можно использовать None, если нужно

    idlist = create_id(cl)
    print(len(idlist))

    print(f"df.iterrows() : {len(df)}")
    print(f"idlist : {len(idlist)}")

    for i, row in df.iterrows():
        # res_date = []

        art_gip_value = row['art-gip'] if 'art-gip' in row and pd.notna(row['art-gip']) else ''
        art_gip_suffix = f"_gip-{art_gip_value}" if art_gip_value else ''

        ad_daBeg = date_beg[i] if pd.notna(date_beg[i]) else ''
        # ad_daBeg = date_beg[i] if pd.notna(date_beg[i]) else current_date.strftime('%Y-%m-%dT%H:%M:%S+03:00')

        

        # if i < cl.num_ads:
        #     df.at[i, 'Id'] = f"{ad_daBeg[:13]}_{articul_list[i]}_{idlist[i]}"
        # else:
        #     df.at[i, 'Id'] = f"bag_idх_{i}"

        ad_part = ad_daBeg[:13] if ad_daBeg else ""
        sep1 = "_" if ad_part else ""

        df.at[i, 'Id'] = f"{ad_part}{sep1}{articul_list[i]}_{idlist[i]}{art_gip_suffix}" if i < cl.num_ads else f"bag_idх_{i}"
        


    return df



def generate_random_coordinates(center_lat, center_lon, radius_km, num_points):
    # Радиус Земли в километрах
    EARTH_RADIUS_KM = 6371.0

    # Преобразуем радиус из километров в радианы
    radius_rad = radius_km / EARTH_RADIUS_KM

    latitudes = []
    longitudes = []

    for _ in range(num_points):
        # Случайный угол азимута в радианах
        azimuth = random.uniform(0, 2 * math.pi)
        # Случайное расстояние от центра в радианах
        distance = radius_rad * math.sqrt(random.uniform(0, 1))

        # Новая широта в радианах
        new_lat_rad = math.asin(math.sin(math.radians(center_lat)) * math.cos(distance) +
                                math.cos(math.radians(center_lat)) * math.sin(distance) * math.cos(azimuth))
        
        # Новая долгота в радианах
        new_lon_rad = math.radians(center_lon) + math.atan2(math.sin(azimuth) * math.sin(distance) * math.cos(math.radians(center_lat)),
                                                            math.cos(distance) - math.sin(math.radians(center_lat)) * math.sin(new_lat_rad))

        # Преобразуем обратно в градусы
        new_lat = math.degrees(new_lat_rad)
        new_lon = math.degrees(new_lon_rad)

        latitudes.append(new_lat)
        longitudes.append(new_lon)

    return latitudes, longitudes

# # Пример использования функции
# center_lat = 55.778669
# center_lon = 37.587964
# radius_km = 60
# num_points = 10

# latitudes, longitudes = generate_random_coordinates(center_lat, center_lon, radius_km, num_points)
# print("Latitudes:", latitudes)
# print("Longitudes:", longitudes)

def read_addresses_file(input_filename):
    addresses = defaultdict(int)

    with open(input_filename, 'r', encoding='utf-8') as input_file:
        # csv_reader = csv.reader(input_file)
        csv_reader = csv.reader(input_file, delimiter=',', quotechar='"')
        next(csv_reader)  # Пропускаем заголовки, если они есть
        for row in csv_reader:
            city, population = row[0], int(row[1])
            addresses[city] = population
    
    return addresses

def read_addresses_with_full_address_file(input_filename):
    addresses_with_full_address = {}
    with open(input_filename, 'r', encoding='utf-8') as input_file:
        # csv_reader = csv.reader(input_file)
        csv_reader = csv.reader(input_file, delimiter=',', quotechar='"')
        next(csv_reader)  # Пропускаем заголовки, если они есть
        for row in csv_reader:
            city, full_address = row[0], row[1]
            addresses_with_full_address.setdefault(city, []).append(full_address)
    
    return addresses_with_full_address

def create_adres(cl):
    res_adres = []
    city_names = []
    input_file_path = f"output_file"
    total_ads = cl.num_ads
    adres_gorod = f"vars/adres_gorod.csv"  # Путь к файлу с городами и полными адресами
    input_filename = f"vars/{cl.k_gorod}"  # Файл с городами
    addresses = read_addresses_file(input_filename)

    total_population = sum(addresses.values())
    for city, population in addresses.items():
        num_ads = int(total_ads * (population / total_population))
        res_adres.extend([city] * num_ads)
        city_names.extend([city] * num_ads)
    
    # Добавляем города для заполнения до общего числа 5000, если сумма городов меньше 5000
    while len(res_adres) < total_ads:
        for city, population in addresses.items():
            if len(res_adres) < total_ads:
                res_adres.append(city)
                city_names.append(city)
            else:
                break
    
    # Добавляем адреса из файла, если они доступны
    addresses_with_full_address = read_addresses_with_full_address_file(adres_gorod)
    for i, city in enumerate(res_adres):
        if city in addresses_with_full_address:
            random_address = random.choice(addresses_with_full_address[city])
            res_adres[i] = f"{random_address}"
    
    output_file = f"output_file/output_adres.txt"  # Имя файла для записи
    with open(output_file, 'w', encoding='utf-8') as file:
         for line in res_adres:
            file.write(line + "\n")  # Запишите строку в файл
    
    return res_adres[:total_ads], city_names[:total_ads]

def create_and_process_title(cl, df, ROOT_DIR):
    for index, row in df.iterrows():
        res_title = []
        input_file_path = f"{ROOT_DIR}/{cl.name}/var/title/{row['Title']}"

        # if 'add_title' in df.columns and isinstance(row['add_title'], str):
        #     input_file_path_add = f"{ROOT_DIR}/{cl.name}/var/title/{row['add_title']}"
        if os.path.exists(input_file_path):
            queries = read_queries_file(input_file_path)
        else:
            continue  
        # queries = read_queries_file(input_file_path)

        total_queries = sum(queries.values())

        for query, count in queries.items():
            if len(query)>50:
                print(f"Длина строки больше 50 символов. : строка /{query}/")
                while True:
                    user_input = input("Продолжить выполнение программы? (да/нет): ").strip().lower()
                    if user_input == 'да':
                        print("Продолжаем выполнение программы.")
                        break
                    elif user_input == 'нет':
                        print("Завершаем выполнение программы.")
                        exit()
                    else:
                        print("Неправильный ввод. Пожалуйста, введите 'да' или 'нет'.")
            num_queries = int(cl.num_ads * (count / total_queries))
            res_title.extend([query] * num_queries)
        while len(res_title) < cl.num_ads:
            for query, count in queries.items():
                if len(res_title) < cl.num_ads:
                    res_title.append(query)
                else:
                    break
        df.at[index, 'Title'] = random.choice(res_title)

    return df

# res_queries
def create_title_list(cl, pretitle=None):
    res_title = []
    total_ads = cl.num_ads
    if pretitle==None:
        input_filename = f"vars/title_zapr.csv"  # Файл с запросами и количеством
    else:
        input_filename = f"vars/title_zapr_{pretitle}.csv"  # Файл с запросами и количеством

    queries = read_queries_file(input_filename)
    total_queries = sum(queries.values())
    
    for query, count in queries.items():
        num_queries = int(total_ads * (count / total_queries))
        res_title.extend([query] * num_queries)
    
    # Добавляем запросы для заполнения до общего числа 5000, если сумма запросов меньше 5000
    while len(res_title) < total_ads:
        for query, count in queries.items():
            if len(res_title) < total_ads:
                res_title.append(query)
            else:
                break
    random.shuffle(res_title)
    output_file = 'output_queries.txt'  # Имя файла для записи
    with open(output_file, 'w', encoding='utf-8') as file:
        for line in res_title:
            file.write(line + "\n")  # Записываем строку в файл
    
    return res_title[:total_ads]  


def read_queries_file(input_filename):
    queries = {}
    with open(input_filename, 'r', encoding='utf-8') as input_file:
        csv_reader = csv.reader(input_file)
        first_row = next(csv_reader)

        # Пробуем интерпретировать второй столбец как число
        try:
            int(first_row[1].strip())
            # Если получилось — это данные, добавим в словарь
            queries[first_row[0].strip()] = int(first_row[1].strip())
        except (ValueError, IndexError):
            # Если не получилось — это заголовок, ничего не делаем
            pass


        # Обрабатываем остальные строки
        for row in csv_reader:
            try:
                query = row[0].strip()
                count = int(row[1].strip())
                queries[query] = count
            except (IndexError, ValueError):
                continue  # Пропускаем некорректные строки

        # next(csv_reader)  # Пропускаем заголовки, если они есть


        # for row in csv_reader:
        #     query, count = row[0], int(row[1])
        #     queries[query] = count
    
    return queries


# --------------------- Даты 

def create_date_list(cl):
    res_date = []

    num_days = cl.num_days
    periods = cl.periods
    num_duplicates = cl.num_ads
    shuffle_list = cl.shuffle_list
    
    start_date = datetime.strptime(cl.date_f, '%Y-%m-%d')
    num_per_day = sum(num_per_day for num_per_day, _, _ in periods)
    max_combinations = cl.num_ads

    for day in range(num_days):
        for period in periods:
            num_per_day, start_hour, end_hour = period
            for _ in range(num_per_day):
                random_hour = random.randint(start_hour, end_hour)
                random_minute = random.randint(0, 59)
                random_second = random.randint(0, 59)
                random_time = time(random_hour, random_minute, random_second)
                
                random_datetime = datetime.combine(start_date + timedelta(days=day), random_time)
                random_datetime = random_datetime.strftime('%Y-%m-%dT%H:%M:%S+03:00')
               
                res_date.append(random_datetime)
    
    if len(res_date) < max_combinations:
        res_date.extend([""] * (max_combinations - len(res_date)))

    if shuffle_list:
        random.shuffle(res_date)

    return res_date

def create_enddate_list(date_list, n):
    end_dates = [(datetime.strptime(date, '%Y-%m-%dT%H:%M:%S+03:00') + timedelta(days=n)).strftime('%Y-%m-%d') for date in date_list]
    return end_dates

def read_city_gtm_file(filename):
    city_gtm = {}
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            city = row['город']
            hour = int(row['час'])
            city_gtm[city] = hour
    return city_gtm

def check_gtm(dates, cities):
    city_gtm_file = 'vars/city_gtm.csv'
    city_gtm = read_city_gtm_file(city_gtm_file)
    print(f"len(city_gtm){len(city_gtm)}")
    corrected_dates = []
    
    for city, date_str in zip(cities, dates):
        try:
            date = parser.parse(date_str)
            city_timezone = city_gtm.get(city)
            if city_timezone is not None:
                current_timezone = date.tzinfo.utcoffset(date).total_seconds() / 3600
                if current_timezone != city_timezone:
                    new_date = date.astimezone(tz.tzoffset(None, city_timezone * 3600))
                    corrected_dates.append(new_date.isoformat())
                else:
                    corrected_dates.append(date_str)
            else:
                corrected_dates.append(date_str)  # Если город не найден, оставляем как есть
        except Exception as e:
            # print(f"Ошибка при обработке даты '{date_str}' для города '{city}': {e}")
            corrected_dates.append(date_str)  # Если дата некорректна, оставляем как есть
    
    return corrected_dates

def create_and_process_date(cl, df):
    res_date = []
    num_days = cl.num_days
    periods = cl.periods
    print(periods)

    num_duplicates = cl.num_ads
    # shuffle_list = cl.shuffle_list
    # shuffle_list = true
    if not periods:
        print('not periods')
        df['DateBegin'] = None
        return df
    else:
        start_date = datetime.strptime(cl.date_f, '%Y-%m-%d')
        num_per_day = sum(num_per_day for num_per_day, _, _ in periods)
        max_combinations = cl.num_ads
        cities = df['Город'].tolist()

        if (len(cities)==num_days):
            print('итоговая таблица равна количеству объяв')
        else:
            print('итоговая таблица не равна количеству объяв, и возможны баги в таблице и стоит все перепроверить!!')

            # while True:
            #         user_input = input("Продолжить выполнение программы? (да/нет): ").strip().lower()
            #         if user_input == 'да':
            #             print("Продолжаем выполнение программы.")
            #             break
            #         elif user_input == 'нет':
            #             print("Завершаем выполнение программы.")
            #             exit()
            #         else:
            #             print("Неправильный ввод. Пожалуйста, введите 'да' или 'нет'.")

        for day in range(num_days):
            for period in periods:
                num_per_day, start_hour, end_hour = period
                for _ in range(num_per_day):
                    random_hour = random.randint(start_hour, end_hour)
                    random_minute = random.randint(0, 59)
                    random_second = random.randint(0, 59)
                    random_time = time(random_hour, random_minute, random_second)
                    
                    random_datetime = datetime.combine(start_date + timedelta(days=day), random_time)
                    random_datetime = random_datetime.strftime('%Y-%m-%dT%H:%M:%S+03:00')
                
                    res_date.append(random_datetime)
        
        if len(res_date) < max_combinations:
            res_date.extend([""] * (max_combinations - len(res_date)))

        # if shuffle_list:
        #     random.shuffle(res_date)


        random.shuffle(res_date)

        
        print(f"len(res_date) {len(res_date)}")
        print(f"len(cities) {len(cities)}")
        print(f"   значения выше должны совпадать!  ")

        corr_date = check_gtm(res_date, cities)


        

        for index, row in df.iterrows():
            # res_date = []
            if index < len(corr_date):

                df.at[index, 'DateBegin'] = corr_date[index]
            else: 
                df.at[index, 'DateBegin'] = None

        

        return df




def generate_random_price():
    return random.randint(11, 50) * 50

def create_pricecol_list(cl):
    res_price = []
    max_combinations = cl.num_ads
    for i in range(1, max_combinations + 1):
 
        res_price.append(generate_random_price())

    return res_price

# Функция для генерации рандомного кода указанной длины
def generate_random_code(length):
    characters = string.digits
    code = ''.join(random.choice(characters) for _ in range(length))
    return code

# Генерация кода из цифр + латинских букв
def generate_random_char_code(length):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def generate_random_hex_color():
    # Генерируем случайные значения для красного, зеленого и синего каналов
    red = random.randint(0, 50)
    green = random.randint(0, 50)
    blue = random.randint(0, 50)

    # Форматируем значения в шестнадцатеричную строку
    hex_color = "#{:02X}{:02X}{:02X}".format(red, green, blue)

    return hex_color


def merge_csv_files(csv_file1, csv_file2, output_filename):
    # Чтение CSV файлов
    df1 = pd.read_csv(csv_file1, dtype=str)
    df2 = pd.read_csv(csv_file2, dtype=str)
    # Очистка данных во втором DataFrame перед преобразованием типов данных
    # df2.fillna("", inplace=True)  # Заполнение пустых значений нулями
    # Или df2.dropna(inplace=True)  # Удаление строк с NaN значениями

    # Проверка типов данных столбцов
    # print("Data types of columns in df1:")
    # print(df1.dtypes)
    # print("\nData types of columns in df2:")
    # print(df2.dtypes)
    
    # Определение общих столбцов
    common_columns = df1.columns.intersection(df2.columns).tolist()
    
    # Проверка типов данных общих столбцов
    # for col in common_columns:
    #     if df1[col].dtype != df2[col].dtype:
    #         # Если типы данных не совпадают, преобразуем тип данных в df2 к типу данных в df1
    #         df2[col] = df2[col].astype(df1[col].dtype)
    #         print(f"Converted column {col} in df2 to {df1[col].dtype}")
    
    # Объединение DataFrame'ов по общим столбцам
    merged_df = pd.merge(df1, df2, on=common_columns, how='outer')
    
    # Сохранение объединенного DataFrame в новый CSV файл
    merged_df.to_csv(output_filename, index=False)


def clean_merged_data(file_path):
    
    # Загрузка объединенных данных из файла
    merged_data = pd.read_csv(file_path, dtype=str)

    # Удаление столбца AvitoStatus
    if 'AvitoStatus' in merged_data.columns:
        merged_data.drop(columns=['AvitoStatus'], inplace=True)

    # Удаление столбцов, в которых нет данных (все значения пустые)
    merged_data.dropna(axis=1, how='all', inplace=True)

    # Замена пустых значений в столбце Availability на "В наличии"
    if 'Availability' in merged_data.columns:
        merged_data['Availability'] = merged_data['Availability'].fillna('В наличии')

    # Сохранение очищенных данных в файл
    merged_data.to_csv(file_path, index=False)


def load_json(row):
    imgparam_path = os.path.join(row)
    with open(imgparam_path, 'r', encoding='utf-8') as file:
        imgparam = json.load(file)
    return imgparam

def generate_text_array(file_path):
    text_array = []
    current_text_block = []
    
    # Чтение файла
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    

    for line in lines:
        line = line.strip()  # Убираем лишние пробелы и символы новой строки
        
        if line.isdigit():  # Если строка содержит только цифру, то это начало нового блока
            # print("строка содержит только цифру, то это начало нового блока")
            if current_text_block:
                # Добавляем текущий блок в массив, если он не пуст
                # print(" Добавляем текущий блок в массив, если он не пуст")
                text_array.append(current_text_block)
                current_text_block = []  # Очищаем текущий блок
        elif line:  # Если строка не пустая
            # Убираем синонимы их строки по разделителю '||' 

            # Определяем тип текста по тегу
            tag, content = extract_tag_and_content(replace_random_variant(line))
            # content = replace_synonyms_in_line(content)


            # combined_textq = replace_random_variant(content_s)
            # content = combined_textq


            if tag and content:
                current_text_block.append({"tag": tag, "text": content})
            
            # current_text_block.append(chosen_text) #сугубо построчно
    
    # Добавляем последний блок в массив (если он не пуст)
    if current_text_block:
        text_array.append(current_text_block)

    return text_array

def replace_random_variant(content_s):
    # Регулярное выражение для поиска конструкции $sin{вариант1 || вариант2 || ...}$sin
    pattern = r"\$sin\{(.*?)\}\$sin"
    
    # Функция для замены
    def replace_match(match):
        # Извлекаем строку между { и }
        options = match.group(1).split(" || ")
        # Возвращаем случайный вариант
        return random.choice(options)
    
    # Заменяем все совпадения в строке
    return re.sub(pattern, replace_match, content_s)



def extract_tag_and_content(line):
    """
    Извлекает тег и текст из строки. 
    Пример строки: '<h1>Заголовок</h1>'
    Возвращает: ('h1', 'Заголовок')
    """
    import re
    match = re.match(r"<(\w+)>(.+?)<\/\1>", line)  # Находим строки вида <тег>текст</тег>
    if match:
        return match.group(1), match.group(2)  # Возвращаем тег и текст
    return None, None

def smart_print(text):
    if sys.stdout.isatty():  # если это терминал
        # sys.stdout.write(f"\r{text}")
        # sys.stdout.flush()
        i=0
    else:  # если это файл (например, лог)
        print(text)

# def natural_sort_key(s):
#     return [int(text) if text.isdigit() else text.lower()
#             for text in re.split(r'(\d+)', s)]

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(r'(\d+)', s)]

def get_selected_images(cl, row, ROOT_DIR):
    input_file_path = os.path.join(ROOT_DIR, cl.name, "var", "img", getattr(row, 'images_folder'))
    cont_dop = int(getattr(row, 'count_img'))
    imgparam = load_json(os.path.join(ROOT_DIR, cl.name, "var", "img", getattr(row, 'imgpar')))

    use_random_images = imgparam.get('randomimg', 1)

    numbered_folders_exist = all(
        os.path.isdir(os.path.join(input_file_path, str(i)))
        for i in range(1, cont_dop + 1)
    )

    selected_images = []

    if numbered_folders_exist:
        # Ищем по подпапкам с номерами
        for i in range(1, cont_dop + 1):
            subfolder_path = os.path.join(input_file_path, str(i))
            subfolder_files = [
                f for f in os.listdir(subfolder_path)
                if os.path.isfile(os.path.join(subfolder_path, f))
            ]
            if subfolder_files:
                selected = random.choice(subfolder_files)
                selected_images.append(os.path.join(str(i), selected))
            else:
                print(f"⚠️ В папке {i} нет изображений!")

        # print(f"перед сортировкой {selected_images}")
        # selected_images = natural_sort_key(selected_images)
        selected_images = sorted(selected_images, key=natural_sort_key)

        # print(f"после сортировки {selected_images}")


        # Обработка non-title
        non_title_indices = []
        if 'non-title' in row and isinstance(row['non-title'], str) and row['non-title'].strip().startswith('['):
            try:
                non_title_indices = ast.literal_eval(row['non-title'])
                # Преобразуем в индексы Python (с 0), отфильтровав некорректные значения
                non_title_indices = [i - 1 for i in non_title_indices if isinstance(i, int) and 1 <= i <= len(selected_images)]
            except Exception as e:
                print(f"⚠️ Ошибка при разборе non-title: {e}")
            # Разделяем изображения
        non_title_images = [selected_images[i] for i in non_title_indices]
        remaining_images = [img for i, img in enumerate(selected_images) if i not in non_title_indices]

        # Перемешиваем оставшиеся
        random.shuffle(remaining_images)    
        # Объединяем: перемешанные + фиксированные в конец
        selected_images = (remaining_images + non_title_images)[:min(cont_dop, len(selected_images))]



    else:
        # Старое поведение: из общей папки
        all_files = [
            f for f in os.listdir(input_file_path)
            if os.path.isfile(os.path.join(input_file_path, f))
        ]

        if use_random_images:
            selected_images = random.sample(all_files, min(cont_dop, len(all_files)))
        else:
            selected_images = sorted(all_files)[:cont_dop]

    # print(selected_images)

    return selected_images


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

START_FILE = os.path.join(BASE_DIR, "start_index.txt")
STOP_FILE = os.path.join(BASE_DIR, 'stop.flag')
GENIMG_FILE = os.path.join(BASE_DIR, 'genimg.flag')

def read_start_index():
    try:
        with open(START_FILE, "r", encoding="utf-8") as f:
            s = f.read().strip()
        if not s:
            return 0
        return int(s)
    except Exception:
        return 0

def write_progress(i):
    try:
        with open(START_FILE, "w", encoding="utf-8") as f:
            f.write(str(i))
    except Exception:
        pass



def create_and_process_img_url(cl, df, ROOT_DIR, nofile=False):
    num_img = 1
    # 1) Есть ли вообще колонка first_img?
    has_first_img = 'first_img' in df.columns

    # 2) Лоадер и кэш JSON будем создавать только при первом реальном использовании
    loader = None
    done = 0

    pause_sec=6
    batch_size=500


# --- читаем стартовый индекс из файла ---
    getotkeda = read_start_index()


    # df['ImageUrls'] = df['ImageUrls'].astype(str)
    # df['ImageUrls'] = df['ImageUrls'].astype(str)

    # print(df['ImageUrls'])

    for index, row in enumerate(df.itertuples(index=False)):
        # если count_img пустой, а ImageUrls уже есть — пропускаем итерацию
        
        # если count_img пустой, а ImageUrls уже есть — пропускаем итерацию
        if pd.notna(df.at[index, 'ImageUrls'] ) and str(df.at[index, 'ImageUrls']).strip() != '':
            continue

        # в row есть articul 
        image_names = []


        if nout:
            otkeda = 10

            if index<otkeda:
                check = True
            else:
                check = False
                
        else:
            if os.path.exists(GENIMG_FILE):
                # на всякий — сохраним позицию, чтобы продолжить потом
                try:
                    otkeda = getotkeda

                    if index<otkeda:
                        check = False
                    else:
                        check = True
                except Exception:
                    pass
            else:
                check = False





        if check:    
            if index % 25 == 0 and index != 0:  
                print(" ")
            
            nofile = False
            percent = int(index / cl.num_ads * 100)
            mrss = f"\r |{index}/{percent}% "
        else:
            nofile = True
            mrss = " |"
            
            if index % 200 == 0 and index != 0:  
                print(" ")

                
        
        if nout:
            percent = int(index / cl.num_ads * 100)
            mrss = f"\r |{index} / {percent}% |"


            sys.stdout.write(mrss)
            sys.stdout.flush()

        address_to_append = f"{cl.address_to_append}/{cl.name}/img_{cl.name_csv}/{index}/"
        output_folder = f"{cl.name}/img_{cl.name_csv}/{index}/"

        if not(nofile):
            # os.makedirs(output_folder, exist_ok=True)
            output_path_cl = os.path.join(ROOT_DIR_OUT, output_folder)

            if os.path.exists(output_path_cl) and os.path.isdir(output_path_cl):
                for f in os.listdir(output_path_cl):
                    full = os.path.join(output_path_cl, f)
                    if os.path.isfile(full):
                        os.remove(full)


        date_prefix = datetime.now().strftime("%d%m%y")




        if 'count_img' in df.columns and pd.notna(getattr(row, 'count_img')) and int(getattr(row, 'count_img')) > 0:
            input_file_path = f"{ROOT_DIR}/{cl.name}/var/img/{getattr(row, 'images_folder')}"
            cont_dop = int(getattr(row, 'count_img'))        
            imgparam = load_json(f"{ROOT_DIR}/{cl.name}/var/img/{getattr(row, 'imgpar')}")
            
            # use_random_images = imgparam.get('randomimg', 1)

            # numbered_folders_exist = all(
            #     os.path.isdir(os.path.join(input_file_path, str(i)))
            #     for i in range(1, cont_dop + 1)
            # )

            # selected_images = []
            
            
            # files = os.listdir(input_file_path)

            

            # if use_random_images == 1:
            #     # Если 'randomimg' равно True или параметр не задан, выбираем случайные изображения
            #     selected_images = random.sample(files, cont_dop)
            # else:
            #     # Если 'randomimg' равно False, выбираем изображения по порядку
            #     selected_images = sorted(files)[:cont_dop]  # Берем первые cont_dop файлов


            selected_images = get_selected_images(cl, row, ROOT_DIR)

            
            file_path_txt_img = f"{ROOT_DIR}/{cl.name}/var/text/to_img_text_{cl.name_csv}.txt"
            # print(file_path_txt_img)
            # text_array_txt_img = generate_text_array(file_path_txt_img)

            if os.path.exists(file_path_txt_img):

                # Если файл существует, загружаем массив текста
                text_array_txt_img = generate_text_array(file_path_txt_img)

                # with open(f"{ROOT_DIR}{cl.name}/var/img/style_txt_to_img.json", 'r') as f: 
                #     style_txt = json.load(f) 
                
                file_with_csv = f"{ROOT_DIR}/{cl.name}/var/img/style_txt_to_img_{cl.name_csv}.json"
                default_file = f"{ROOT_DIR}/{cl.name}/var/img/style_txt_to_img.json"

                if os.path.exists(file_with_csv):
                    with open(file_with_csv, 'r') as f:
                        style_txt = json.load(f)
                        # print(f"style {file_with_csv}")
                else:
                    with open(default_file, 'r') as f:
                        style_txt = json.load(f)
                        # print("default style")

            else:
                # Если файл не существует, можно выполнить альтернативные действия
                # print(f"Файл to_img_txt_{cl.name_csv}.txt не найден. текст на доп картинки не будет размещен!")    
                text_array_txt_img = [] 
                style_txt = None    
                             

            
            # num_img = 0
            for i in range(cont_dop):
                nameimg = f"{date_prefix}_{getattr(row, 'images_folder')}_{i+1}_{num_img}.jpg"
                image_names.append(f"{address_to_append}/{nameimg}")
            #    if text_array_txt_img

                if i < len(text_array_txt_img):
                    text_block = text_array_txt_img[i]
                else:
                    text_block = []

                if not(nofile):


                    img.process_image_row(input_file_path, selected_images[i], output_folder, nameimg, imgparam, text_block, style_txt, i, cl.name)
                
                num_img += 1

                # print(f"{cl.name_csv}/ картинка {new_image_name} колаж для {index} объяв создана")

        if 'count_dop_img' in df.columns and pd.notna(getattr(row, 'count_dop_img')) and int(getattr(row, 'count_dop_img')) > 0:

            cont_dop = int(getattr(row, 'count_dop_img'))
            
            imgparam = load_json(f"{ROOT_DIR}/{cl.name}/var/img/{getattr(row, 'imgpar')}")
            
            input_file_path_dop_img = f"{ROOT_DIR}/{cl.name}/var/img/{getattr(row, 'dop_images_folder')}"

            files = os.listdir(input_file_path_dop_img)

            use_random_images = imgparam.get('randomimg', 1)

            if use_random_images == 1:
                # Если 'randomimg' равно True или параметр не задан, выбираем случайные изображения
                selected_images = random.sample(files, cont_dop)
            else:
                # Если 'randomimg' равно False, выбираем изображения по порядку
                selected_images = sorted(files)[:cont_dop]  # Берем первые cont_dop файлов





            for i in range(cont_dop):
                nameimg = f"{date_prefix}_{getattr(row, 'dop_images_folder')}_{i+1}_{num_img}.jpg"
                image_names.append(f"{address_to_append}/{nameimg}")

                if not(nofile):
                    img.process_image_row(input_file_path_dop_img, selected_images[i], output_folder, nameimg, imgparam,)
                
                num_img += 1

                # print(f"{cl.name_csv}/ картинка {new_image_name} колаж для {index} объяв создана")

        if 'first_img' in df.columns and isinstance(getattr(row, 'first_img'), str) and getattr(row, 'first_img').endswith('.json'):
            imgparam = load_json(f"{ROOT_DIR}/{cl.name}/var/img/{getattr(row, 'first_img')}")
            column_data = []
            if 'list_col_to_frst_img' in imgparam and imgparam['list_col_to_frst_img']:
                col_file_path = f"{ROOT_DIR}/{cl.name}/var/img/{imgparam['list_col_to_frst_img']}"
                # print(col_file_path)
                # print(col_file_path)
                # print(col_file_path)
                if os.path.exists(col_file_path):
                    columns_to_use = load_columns_from_file(col_file_path)
                    pos = {c: df.columns.get_loc(c) for c in columns_to_use if c in df.columns}

                    for i, col in enumerate(columns_to_use):
                        if col in df.columns:
                            # безопасное извлечение ячейки из row
                            try:
                                val = row[col] 
                            except Exception:
                                try:
                                    val = getattr(row, col)    # itertuples(name=...) — namedtuple
                                except Exception:
                                    val = row[pos[col]]        # tuple/ndarray (df.values / itertuples(name=None))
                            entry = {"name": col, "value": val}

                            column_data.append(entry)
            
            # new_image_name = f"{getattr(row, 'articul')}_{index}.jpg"
            if cl.name == "sborpk":
                new_image_name = f"{getattr(row, 'full_art')}_{index}.jpg".replace(" ", "_") #для сборки ПК , в articul соркащеный артикул
            else:
                new_image_name = f"1_{date_prefix}_{getattr(row, 'articul')}_{index}.jpg".replace(" ", "_") #для сборки ПК , в articul соркащеный артикул


            # if True:
            if not(nofile):
                # has_first_img = False
                # ------------------------------
                # ВЕТКА first_img (условная)
                # ------------------------------
                if has_first_img:
                    first_img_val = getattr(row, 'first_img')  # у itertuples это безопасно, т.к. колонка точно есть
                    

                    if isinstance(first_img_val, str) and first_img_val.strip():
                        json_path = os.path.join(ROOT_DIR, cl.name, "var", "img", first_img_val)
                        if os.path.exists(json_path):
                            # ленивое создание кэшей только когда реально нужно
                            if loader is None:
                                loader = ScriptLoader(maxsize=512, func_name="execute_task")

                            imgparam = load_json_cached(json_path) or {}
                            script_name = imgparam.get('script') or imgparam.get('script_path')
                            if script_name:
                                script_path = script_name if os.path.isabs(script_name) else \
                                            os.path.join(ROOT_DIR, cl.name, "var", "img", script_name)
                                try:
                                    exec_func = loader.get(script_path)
                                    # exec_func = loader.get(script_path)

                                    # # Проверка всех аргументов перед вызовом
                                    # args_to_check = {
                                    #     'ROOT_DIR': ROOT_DIR,
                                    #     'ROOT_DIR_OUT': ROOT_DIR_OUT,
                                    #     'new_image_name': new_image_name,
                                    #     'cl': cl,
                                    #     'imgparam': imgparam,
                                    #     'column_data': column_data,
                                    #     'index_n': index
                                    # }

                                    # # Выводим типы ВСЕХ аргументов
                                    # for arg_name, arg_value in args_to_check.items():
                                    #     print(f"[DEBUG] {arg_name}: type={type(arg_value)}, value={arg_value}")

                                    # try:
                                    #     exec_func(**args_to_check)
                                    #     done += 1
                                    # except TypeError as e:  # Ловим конкретно TypeError
                                    #     print(f"[WARN] first_img script failed at row {index}")
                                    #     print(f"Ошибка: {e}")
                                        
                                    #     # Анализируем traceback для точного места
                                    #     import traceback
                                    #     tb = traceback.extract_tb(e.__traceback__)
                                        
                                    #     # Ищем в трейсбэке упоминания None
                                    #     for frame in tb:
                                    #         if 'None' in str(frame.line) or 'none' in str(frame.line).lower():
                                    #             print(f"  → Проблема в файле {frame.filename}, строка {frame.lineno}")
                                    #             print(f"  → Код: {frame.line}")
                                        
                                    #     # Если не нашли в трейсбэке, проверяем наши аргументы
                                    #     for arg_name, arg_value in args_to_check.items():
                                    #         if arg_value is None:
                                    #             print(f"  → Аргумент '{arg_name}' = None (возможно проблема здесь)")
                                    exec_func(
                                        ROOT_DIR=ROOT_DIR,
                                        ROOT_DIR_OUT=ROOT_DIR_OUT,
                                        new_image_name=new_image_name,
                                        cl=cl,
                                        imgparam=imgparam,
                                        column_data=column_data,   # при необходимости собери тут из imgparam
                                        index_n=index
                                    )
                                    done += 1
                                except Exception as e:
                                    print(f"[WARN] first_img script failed at row {index}: {e}")
                        # если файла JSON нет — молча пропускаем (как просил)
                    # если пусто/NaN — тоже пропускаем без действий
                # if not has_first_img: ничего не делаем вообще — ни лоадера, ни чтений

                # Батчи: чистка/пауза (по суммарно сделанным работам)
        
        

            # new_image_name = f"1_{getattr(row, 'images_folder')}_{index}.jpg"
            # print(f"{cl.name_csv}/ картинка {new_image_name} колаж для {index} объяв создана")

            # image_names
            # if random.random() < 0.7:  # 30% вероятность выполнения

            
            if cl.name in ("stroy", "dezi"):
                # print('первая из масс', end="")
                pass
            else:
                image_names.insert(0, f"{address_to_append}/{new_image_name}")
            # num_img += 1


        imgurl_list = " | ".join(image_names)


        df.at[index, 'ImageUrls'] = str(imgurl_list)

        # мягкая остановка по флагу
        if os.path.exists(STOP_FILE):
            # на всякий — сохраним позицию, чтобы продолжить потом
            try:
                write_progress(index)  # или index+1, как ты решил
            except Exception:
                pass
            print("⛔ Получен стоп-флаг. Выходим мягко.")
            return df  # или sys.exit(0) если ты в верхнем уровне

        write_progress(index)

    gc.collect()


    print("Картинки сгенерированы")

    return df


# читать путь к файлу из таблицы, импортировать файл и вызывать его функцию execute_task().
def load_and_execute_script(script_path, ROOT_DIR, ROOT_DIR_OUT, new_image_name, cl, parameters, column_data, index_n):
    # Проверяем, существует ли указанный файл
    if not os.path.exists(script_path):
        print(f"Файл {script_path} не найден.")
        return
    
    # Загружаем модуль из файла
    module_name = os.path.splitext(os.path.basename(script_path))[0]
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    # Вызываем функцию `execute_task`, если она определена в модуле
    if hasattr(module, 'execute_task'):
        module.execute_task(ROOT_DIR, ROOT_DIR_OUT, new_image_name, cl, parameters, column_data, index_n)
    else:
        print(f"Функция `execute_task` не найдена в {script_path}")



def read_columns_to_delete(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        columns_to_delete = [line.strip() for line in file if line.strip()]
    return columns_to_delete

def delete_columns(df, columns_to_delete):
    df = df.drop(columns=[col for col in columns_to_delete if col in df.columns])
    return df



def generate_html_from_df(df, output_path):
    # Если строк больше 5, выбираем случайные 5 строк, иначе берем все
    if len(df) > 100:
        df_sample = df.sample(n=100, random_state=1)
    else:
        df_sample = df
    
    # Начинаем создание HTML
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Sample Data</title>
        <style>


        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            display: flex;
            flex-wrap: wrap;
            background-color: #303030;
        }
            
     
           
        .column-value { 
            margin-left: 10px; 
            
        }
            
        .column-value {
            margin-left: 10px;
            padding: .3em;
        }

        .column-value img {
           
        }

        .column-value img:first-child{
       
        }
        .column-title {
            font-weight: bold;
            margin-bottom: 5px;
            background-color: #a1a1a1;
            /* margin: 5px 5px 7px 0; */
            padding: .3em;
        }
        .column-title {
            flex: 0 0 auto;
            min-width: 50px;         /* чтобы заголовки не прыгали */
            font-weight: 600;
            }

            .column-value {
  flex: 1 1 auto;
}


.row-min.title .column-value {
  font-size: 150%;
  font-weight: 700;
  color: #222;
}

/* Price — 125% и жирный */
.row-min.price .column-value {
  font-size: 125%;
  font-weight: 700;
  color: #222;
}

.row-min.title { order: 1; }
.row-min.price { order: 2; }
        .row-min {
            display: flex;
            flex-wrap: wrap;
            margin: 5px;
         
            order: 3;
        }
        .row {
                margin-bottom: 20px;
            display: flex;
            flex-wrap: wrap;
            flex: 1 1 48%;
            min-width: 400px;
            background-color: #bebebe;
            margin: .5em;
            gap: 8px;
        }

        .gallery-container {
    position: relative;
    width: 100%;
    height: 500px;
}

.column-value.galer {
    display: flex;
    overflow: hidden;
    height: 100%;
}

.column-value.galer img {
    display: none;
    height: 100%;
    object-fit: contain;
}

.controls {
    position: absolute;
    top: 50%;
    width: 100%;
    display: flex;
    justify-content: space-between;
    transform: translateY(-50%);
}

button {
    background-color: rgba(0, 0, 0, 0.5);
    color: white;
    border: none;
    padding: 10px;
    cursor: pointer;
}

button:hover {
    background-color: rgba(0, 0, 0, 0.8);
}

ul {
    list-style-type: none;

    }
        </style>
    </head>
    <body>
    """

    for index, row in df_sample.iterrows():
        html_content += '<div class="row">\n'
        for col in df.columns:
            # Получаем значение ячейки
            cell_value = row[col]
            addclass = ""
            if col == "Title":
                addclass += " title"
            if col == "numad":
                addclass += " title"


            if col=="Price":
                addclass = addclass + " price"


            if pd.isna(cell_value) or cell_value == '':
                continue  # переход к следующей итерации, если значение пустое или NaN
            html_content += f'<div class="row-min{addclass}">\n'
            html_content += f'<div class="column-title">{col}:</div>\n'
            
            
            
            # Проверяем, если в ячейке есть ссылки на изображения
            if isinstance(cell_value, str) and 'http' in cell_value and '.jpg' in cell_value:
                # Разделяем ссылки по разделителю ' | '
                image_links = cell_value.split(' | ')
                
                # Фильтруем только те ссылки, которые оканчиваются на .jpg
                image_links = [link for link in image_links if link.lower().endswith('.jpg')]
                
                # Генерируем HTML для первой картинки с шириной 500px
                if image_links:
                    first_image = image_links[0]
                    img_tags = [f'<img src="{first_image}" alt="image" style="width:500px; height:auto; margin:5px;">']
                    
                    # Генерируем HTML для остальных картинок с шириной 100px
                    other_images = image_links[1:]
                    img_tags.extend([f'<img src="{link}" alt="image" style="max-width:500px; max-height:auto; margin:5px;">' for link in other_images])
                    
                    # Соединяем теги <img> в одну строку
                    img_tags_html = ''.join(img_tags)
                    
                    # Вставляем теги <img> в HTML
                    html_content += f'<div class="gallery-container"><div class="column-value galer">{img_tags_html}</div><div class="controls"><button class="prev-btn">Назад</button><button class="next-btn">Вперед</button></div></div>\n'
            else:
                # Вставляем значение ячейки как текст
                html_content += f'<div class="column-value">{cell_value}</div>\n'
            html_content += '</div>\n'
        html_content += '</div>\n'

    # Завершаем создание HTML
    html_content += """
    <script>
    document.addEventListener("DOMContentLoaded", function() {
    // Получаем все контейнеры с галереями
    const galleries = document.querySelectorAll('.gallery-container');

    galleries.forEach(gallery => {
        const images = gallery.querySelectorAll('.column-value.galer img');
        const prevButton = gallery.querySelector('.prev-btn');
        const nextButton = gallery.querySelector('.next-btn');
        let currentImageIndex = 0;

        // Показать первую картинку
        images[currentImageIndex].style.display = 'block';

        // Функция для переключения картинок
        function showImage(index) {
            images[currentImageIndex].style.display = 'none'; // Скрываем текущую картинку
            currentImageIndex = index; // Обновляем индекс
            images[currentImageIndex].style.display = 'block'; // Показываем новую картинку
        }

        // Кнопка "Назад"
        prevButton.addEventListener('click', function() {
            let newIndex = currentImageIndex - 1;
            if (newIndex < 0) {
                newIndex = images.length - 1; // Если дошли до начала, переходим к последней картинке
            }
            showImage(newIndex);
        });

        // Кнопка "Вперед"
        nextButton.addEventListener('click', function() {
            let newIndex = currentImageIndex + 1;
            if (newIndex >= images.length) {
                newIndex = 0; // Если дошли до конца, переходим к первой картинке
            }
            showImage(newIndex);
        });
    });
});


    </script>
    </body>
    </html>
    """

    # Записываем HTML в файл
    with open(output_path, 'w', encoding='utf-8') as file:
        file.write(html_content)

    print(f"HTML файл успешно создан: {output_path}")


def generate_wp_from_df(df, output_path, params):
    # Если строк больше 5, выбираем случайные 5 строк, иначе берем все
    if len(df) > 20:
        df_sample = df.sample(n=20, random_state=1)
    else:
        df_sample = df
    

    df_wp = pd.DataFrame()

    df_wp['Артикул'] = df_sample['Id']

    df_wp['Имя'] = df_sample['Title']
    df_wp['Описание'] = df_sample['Description']
    df_wp['Город'] = df_sample['Город']
    df_wp['Базовая цена'] = df_sample['Price']
    
    # Переносим и обрабатываем данные в новом DataFrame
    df_wp['Изображения'] = pd.NA 
    # df_wp['Изображения'] = df_sample['ImageUrls'].str.replace(' \| ', ', ', regex=False)  # Заменяем " | " на ", "

    df_wp['Метки'] = pd.NA          # Добавляем пустой столбец Метки
    df_wp['Категории'] = pd.NA      # Добавляем пустой столбец Категории
    df_wp['Краткое описание'] = pd.NA  # Добавляем пустой столбец Краткое описание





    for index, row in df_wp.iterrows():


        краткое_описание = row['Описание'][:250]  # Срезаем до 250 символов
        # Добавляем троеточие, если длина больше 250
        if len(row['Описание']) > 250:
            краткое_описание += '...'
        # Записываем в новый столбец Краткое описание
        df_wp.at[index, 'Краткое описание'] = краткое_описание

        df_wp.at[index, 'Метки'] = params.info_dict['CompanyName']


        df_wp.at[index, 'Изображения'] = df_sample.at[index, 'ImageUrls'].replace(' | ', ', ')

        df_wp.at[index, 'Категории'] = params.cat_wp

        phone_number_str = str(params.info_dict['ContactPhone'])
        formatted_number = f"+7 ({phone_number_str[1:4]}) {phone_number_str[4:7]}-{phone_number_str[7:9]}-{phone_number_str[9:11]}"
        dop_info = f"<p>Для заказа без наценок за маркетинг: <a href=\"{params.info_dict['ContactPhone']}\">{formatted_number}</a> {params.info_dict['ManagerName']}</p>"

        df_wp.at[index, 'Описание'] = dop_info + row['Описание']


 



    df_wp.to_csv(output_path, index=False)

    print(f"WP файл успешно создан: {output_path}")



def replace_grand_values(df: pd.DataFrame) -> pd.DataFrame:
    def replace_in_cell(cell):
        if not isinstance(cell, str):
            return cell

        def repl(match):
            x, y, z = map(int, match.groups())
            possible_values = list(range(x, y + 1, z))
            return str(random.choice(possible_values)) if possible_values else '0'

        pattern = r'grand\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)'
        return re.sub(pattern, repl, cell)

    return df.applymap(replace_in_cell)


def load_columns_from_file(file_path):
    """Загружает список колонок из файла."""
    with open(file_path, 'r', encoding='utf-8') as file:
        columns = [line.strip() for line in file]
    return columns



def make_csv_url_simple(sheet_url):
    """
    Преобразует URL Google Sheets в прямую ссылку на CSV без дополнительных библиотек.
    """
    # Ищем ID таблицы между '/d/' и следующим '/'
    try:
        start = sheet_url.index("/d/") + 3
        end = sheet_url.index("/", start)
        spreadsheet_id = sheet_url[start:end]
    except ValueError:
        raise ValueError("Невозможно определить ID таблицы из URL")

    # Ищем gid: в query (?gid=...) или в fragment (#gid=...)
    sheet_gid = None
    if "?gid=" in sheet_url:
        sheet_gid = sheet_url.split("?gid=")[1].split("&")[0]
    elif "#gid=" in sheet_url:
        sheet_gid = sheet_url.split("#gid=")[1].split("&")[0]
    else:
        raise ValueError("Невозможно определить gid листа из URL")

    # Формируем ссылку для CSV
    csv_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv&gid={sheet_gid}"
    return csv_url
