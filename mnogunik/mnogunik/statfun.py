# -*- coding: utf-8 -*-

import pandas as pd
import sys
import openpyxl
from openpyxl import load_workbook
import os
import shutil
import json
import requests
import datetime
import math
import time
from config import ROOT_DIR_OUT, ROOT_DIR, ROOT_URL_OUT


# ROOT_DIR = 'C:/proj/'
st_par = {
    'name': 'srg',  # имя клиента и его папки
    'name_kont': 'Запросов контактов на Avito',  # имя столбца для контактов
    'name_pros': 'Просмотров на Avito',  # имя столбца для просмотров
    'name_izbr': 'Добавлений в избранное на Avito',  # имя столбца для избранного

    'kont': 1,  # минимальное количество контактов
    'izbr': 1,  # минимальное количество добавлений в избранное
    'pros': 1,  # минимальное количество просмотров

    'name_idstat': 'Номер в Avito',  # имя столбца с ID в статистике
    'name_idads': 'AvitoId',  # имя столбца с ID в объявлениях

    'file_stat': f"stat_srg.xlsx",  # файл со статистикой
    'file_ads': f"ads_srg.xlsx",  # файл с объявлениями
    'filtered_ads': 'filtered_ads.csv'  # файл для сохранения отфильтрованных данных
}

def kick_nulstat(st_par, param=None):
    print('Загрузка данных из файлов')
    # print('  1   ')
    # print(f"  1   {st_par['file_stat']}")
    # Загрузка данных из файлов
    try:
        stat_data = pd.read_excel(st_par['file_stat'], dtype=str)
        # print(stat_data.columns)
        # print(stat_data)

       
    except Exception as e:
        print(f"Произошла ошибка при считывании файла pd.read_excel(st_par['file_stat']): {e}")
        print(f" ---------------------     ------------------        ------------")
        # ads_data = pd.read_excel(f"xls/{st_par['file_ads']}")
        sys.exit(1)  # Завершение программы с кодом 1 (ошибка)
    # print('  2   ')
    ads_data = pd.read_excel(st_par['file_ads'],dtype=str)
    # print(ads_data.columns)
    # print(ads_data)
    ads_data.to_csv(f"{ROOT_DIR}/{param.name}/stat/all_ads_{param.name_csv}/all_ads.csv", index=False)
    # print('  3   ')
    # print('Объединение данных по столбцам AvitoId и Номер в Avito')
    # Объединение данных по столбцам AvitoId и Номер в Avito
    merged_data = pd.merge(ads_data, stat_data, left_on=st_par['name_idads'], right_on=st_par['name_idstat'], how='inner')
    # print('  4   ')

    merged_data.to_csv(f"{ROOT_DIR}/{param.name}/stat/merged_data_{param.name_csv}.csv", index=False)
    print('Фильтрация строк в объединенных данных по условиям')
    # print(merged_data[[st_par['name_pros'], st_par['name_kont'], st_par['name_izbr']]].head())  # Посмотреть первые строки
    # print(st_par)  # Посмотреть значения параметров
    # Фильтрация строк в объединенных данных по условиям
    filtered_data = merged_data[
        (merged_data[st_par['name_pros']].astype(int) > int(st_par['pros'])) |
        (merged_data[st_par['name_kont']].astype(int) > int(st_par['kont'])) |
        (merged_data[st_par['name_izbr']].astype(int) > int(st_par['izbr']))
    ]

    filtered_data.to_csv(f"{ROOT_DIR}/{param.name}/stat/filtered_data_{param.name_csv}.csv", index=False)
    print('Оставляем только нужные столбцы')
    # Оставляем только нужные столбцы
    filtered_ads_data = filtered_data[ads_data.columns]

    # print('Сохранение отфильтрованных данных в новый файл')
    # # Сохранение отфильтрованных данных в новый файл
    # filtered_ads_data.to_excel(f"xls/{st_par['filtered_ads']}", index=False)

    if (param==None):
        print('Сохранение отфильтрованных данных в новый файл в формате CSV')
        # Сохранение отфильтрованных данных в новый файл в формате CSV
        filtered_ads_data.to_csv(f"xls/{st_par['filtered_ads']}", index=False)
    else:
        print('Сохранение отфильтрованных данных в новый файл в формате CSV в папку проекта')
        # Сохранение отфильтрованных данных в новый файл в формате CSV
        filtered_ads_data.to_csv(f"{ROOT_DIR}/{param.name}/stat/{st_par['filtered_ads']}", index=False)


def process_stat(st_par, params, txt, stt, ROOT_DIR, output_file_path):
    """
    Обрабатывает статистику и объединяет файлы, если они существуют.
    """
    file_stat = st_par['file_stat']
    file_ads = st_par['file_ads']
    
    if not (os.path.exists(file_stat) and os.path.exists(file_ads)):
        print(f"Файл {file_stat} не найден. Программа создаст только новые объявления!")
        return
    
    try:
        print('Начало обработки статистики')
        kick_nulstat(st_par, params)
        print("Объединению")

        csv_filename1 = output_file_path
        csv_filename2 = os.path.join(ROOT_DIR, params.name, "stat", st_par['filtered_ads'])
        output_filename_stat = os.path.join(ROOT_DIR, params.name, f"statads_{params.name}_{params.name_csv}_{params.date_f}_{params.num_ads}.csv")
        
        txt.merge_csv_files(csv_filename1, csv_filename2, output_filename_stat)
        txt.clean_merged_data(output_filename_stat)
        
    except Exception as e:
        print(f"Ошибка при обработке статистики: {e}")

# Вызов функции
# kick_nulstat(st_par)


#ревизия папки с данными. после загрузки туда кидается файл
def process_ads_folder(ROOT_DIR, params):

    folder_path = f"{ROOT_DIR}/{params.name}/stat/ads_{params.name_csv}/"

    arh_path = os.path.join(folder_path, "arh")
    
    # Создаем папку `arh`, если её нет
    os.makedirs(arh_path, exist_ok=True)

    # Получаем список файлов в папке, кроме `arh/`
    all_files = [f for f in os.listdir(folder_path) if f != "arh"]
    
    # Фильтруем CSV-файлы (кроме папок)
    csv_files = [f for f in all_files if f.endswith(".csv")]
    
    # Если нет новых файлов кроме `all_ads.csv`, выходим
    if len(csv_files) <= 1:
        print("Нет новых файлов для обработки.")
        return

    # Очистка папки `arh/`
    for old_file in os.listdir(arh_path):
        old_file_path = os.path.join(arh_path, old_file)
        os.remove(old_file_path)

    # Перемещение всех CSV в `arh/`
    for file in csv_files:
        src_path = os.path.join(folder_path, file)
        dst_path = os.path.join(arh_path, file)
        shutil.move(src_path, dst_path)

    # Объединение всех CSV из `arh/` в один
    merged_df = pd.concat(
        [pd.read_csv(os.path.join(arh_path, file), dtype=str) for file in csv_files],
        ignore_index=True
    )
    # Список всех столбцов
    cols = merged_df.columns.tolist()
    # print(merged_df)
    # Указываем нужный порядок
    front_cols = ['Id', 'AvitoId']
    remaining_cols = [col for col in cols if col not in front_cols]
    
    # Новый порядок столбцов
    new_order = front_cols + remaining_cols
    merged_df = merged_df[new_order]

    # Сохранение нового `all_ads.csv`
    merged_csv_path = os.path.join(folder_path, "all_ads.csv")

    merged_df['AvitoId'] = merged_df['AvitoId'].fillna('')
    merged_df['AvitoId'] = merged_df['AvitoId'].astype(str)

    merged_df.to_csv(merged_csv_path, index=False)

    print(f"Файлы объединены и сохранены в {merged_csv_path}")




# Функция получения AvitoId
def get_avitoid(access_token, avito_idf):
    headers = {"Authorization": f"Bearer {access_token}"}
    stats_data = []
    
    for i in range(0, len(avito_idf), 200):
        batch_idf = "|".join([str(avito_id).replace(",", "%2C") for avito_id in avito_idf[i:i + 200]]) # Объединяем ID через "|"
        # print(batch_idf)
        url = f"https://api.avito.ru/autoload/v2/items/avito_ids?query={batch_idf}"

        # response = requests.get(url, headers=headers)  # Используем GET-запрос
        response = safe_get(url, headers)
        if response is None:
            print(f"❌ Не удалось получить данные для batch: {batch_idf}")
            continue
         

        # print(response)
        # print(f"{url}{headers}")

        
        if response.status_code == 200:
            data = response.json().get("items")
            dataprint = response.json()

            # print(dataprint)
            missing_ids = []
            for item in dataprint['items']:
                ad_id = item.get('ad_id')
                avito_id = item.get('avito_id')
                
                if avito_id is not None:
                    print(f"✅ Найдено: {ad_id} → AvitoId: {avito_id}")
                else:
                    print(f"❌ не найдено: {ad_id} → AvitoId не найдено")
                    missing_ids.append(ad_id)

            if missing_ids:
                print(f"❌ Всего не найдено AvitoId: {len(missing_ids)}")
                print("   ID:", ", ".join(missing_ids))


            stats_data.extend(data)
        else:
            print(f"❌ Ошибка {response.status_code}: {response.text}")

    
    return stats_data

def get_ad_stats_custom_range(access_token,USER_ID, avito_ids, days_back=10):
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    stats_data = []

    STATS_URL = f"https://api.avito.ru/stats/v1/accounts/{USER_ID}/items"
    today = datetime.date.today()
    # today = date.today()
    date_to = today - datetime.timedelta(days=1)
    date_from = date_to - datetime.timedelta(days=days_back - 1)

    # Проходим по каждому дню (можно потом заменить на недели для оптимизации)
    for single_date in (date_from + datetime.timedelta(n) for n in range(days_back)):
        day_str = single_date.strftime("%Y-%m-%d")
        # print(f"📅 Получаем данные за {day_str}")
        
        for i in range(0, len(avito_ids), 200):
            batch_ids = [int(x) for x in avito_ids[i:i + 200]]
            payload = {
                "dateFrom": day_str,
                "dateTo": day_str,
                "fields": ["uniqViews", "uniqContacts", "uniqFavorites"],
                "itemIds": batch_ids,
                "periodGrouping": "day"
            }

            # response = requests.post(STATS_URL, headers=headers, json=payload)

            response = safe_post(STATS_URL, headers, payload)

            if response.status_code == 200:
                day_data = response.json().get("result", {}).get("items", [])
                for item in day_data:
                    for stat in item.get("stats", []):
                        stats_data.append({
                            "itemId": item["itemId"],
                            "date": stat.get("date"),
                            "views": stat.get("uniqViews", 0),
                            "contacts": stat.get("uniqContacts", 0),
                            "favorites": stat.get("uniqFavorites", 0)
                        })
            else:
                print(f"⚠️ Ошибка {response.status_code} для {day_str}: {response.text}")

    return stats_data



def process_stat_api(st_par, params, txt, stt, ROOT_DIR, output_file_path):
    """
    Обрабатывает статистику и объединяет файлы, если они существуют.
    """

    file_ads = f"{ROOT_DIR}/{params.name}/stat/ads_{params.name_csv}.xlsx"
    
    print("Загрузка - Файл api.json")
    # Задаем путь к файлу
    api_json_path = f"{ROOT_DIR}/{params.name}/var/api.json"

    # Проверяем существование файла
    if os.path.isfile(api_json_path):
        with open(api_json_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        # Получаем данные по ключу params.name_csv
        if params.name_csv in data:
            CLIENT_ID = data[params.name_csv]["CLIENT_ID"]
            CLIENT_SECRET = data[params.name_csv]["CLIENT_SECRET"]
            USER_ID = data[params.name_csv]["USER_ID"]

            TOKEN_URL = "https://api.avito.ru/token"
            

            print("✅ Данные api загружены успешно.")
        else:
            print(f"❌ Ключ '{params.name_csv}' не найден в файле.")
    else:
        print("❌ Файл api.json отсутствует.")

    # Функция получения токена
    def get_access_token():
        data = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        }
        response = requests.post(TOKEN_URL, data=data)
        return response.json().get("access_token")

    # Получаем токен
    access_token = get_access_token()



    # Чтение файлов
    df_before_ads = pd.read_excel(st_par['file_before_ads'], dtype=str).set_index("Id")
    df_ads = pd.read_excel(st_par['file_ads'], dtype=str).set_index("Id")
    df_ads.update(df_before_ads)



    df_combined = pd.concat([df_ads, df_before_ads[~df_before_ads.index.isin(df_ads.index)]])

    # Объединение (если строки нужно объединить друг за другом)
    ads_df = df_combined.reset_index()

    # print(ads_df)
    # avito_idf = ads_df["Id"].tolist()

    #извлекаем id у которых нет пары AvitoId
    avito_idf = ads_df[ads_df["AvitoId"].isna()]["Id"].tolist()

    # print(avito_idf)

    #ПОлучаем avitoId к айдишникам
    Aviid_list = get_avitoid(access_token, avito_idf)

    # print(f"Aviid_list {Aviid_list}")
    # print("3")
    # print(ads_df["AvitoId"])
    avito_dict = {item['ad_id']: item['avito_id'] for item in Aviid_list}

    # print(f"avito_dict {avito_dict}")
    # print("4")
    # print(ads_df["AvitoId"])
    
    # вставка авитоайди 
    ads_df['AvitoId'] = ads_df.apply(
        lambda row: avito_dict.get(row['Id'], row['AvitoId']),
        axis=1
    )

    # print("5")
    # print(ads_df["AvitoId"])
    # Убираем возможную лишнюю ".0" для чисел
    # Преобразуем к строке, убираем .0, если есть
    # ads_df['AvitoId'] = ads_df['AvitoId'].apply(lambda x: str(int(x)) if pd.notna(x) else x)
    # print(ads_df['AvitoId'].head(10))  # Убедись, что только строки без .0
    # print(ads_df['AvitoId'].dtype) 
    # print(ads_df)

    # ads_df['AvitoId'] = ads_df['AvitoId'].fillna('1')

    # print(ads_df["Price"].dtype)
    # print(ads_df)
    # print(ads_df["AvitoId"])
    # avito_ids = ads_df["AvitoId"].tolist()
    avito_ids = [x for x in ads_df["AvitoId"].tolist() if x is not None and str(x).strip() != '' and (not isinstance(x, float) or not math.isnan(x))]
    # print(avito_ids)
    # print(avito_ids)
    # print("avito_ids end")
    stats_list = get_ad_stats_custom_range(access_token,USER_ID, avito_ids,45)
    # print(stats_list)

    stats_df = pd.DataFrame(stats_list)

 
    stats_summary = stats_df.groupby("itemId")[["views", "contacts", "favorites"]].sum().reset_index()

    stats_summary.rename(columns={"uniqViews": "views", "uniqContacts": "contacts", "uniqFavorites": "favorites"}, inplace=True)

    # 1. Удаляем старые колонки, если они есть
    ads_df = ads_df.drop(columns=["views", "contacts", "favorites"], errors="ignore")

    ads_df['AvitoId'] = ads_df['AvitoId'].astype(str)
    stats_summary['itemId'] = stats_summary['itemId'].astype(str)

    # merged_df = ads_df.merge(stats_summary, left_on="AvitoId", right_on="itemId", how="left").drop(columns=["itemId"])

    # 3. Объединяем по AvitoId <-> itemId
    merged_df = ads_df.merge(
        stats_summary.rename(columns={"itemId": "AvitoId"}),  # чтобы ключи совпали
        on="AvitoId",
        how="left"
    )

    # merged_df.to_csv("ads_updated.xlsx", index=False)

    # print("Колонки в merged_df:", merged_df.columns.tolist())

    filtered_df = merged_df[
        (merged_df["views"] > 0) |
        (merged_df["contacts"] > 0) |
        (merged_df["favorites"] > 0)
    ]

    filtered_file_path = f"{ROOT_DIR}/{params.name}/stat/filtered_{params.name_csv}_ads.csv"


    filtered_df.to_csv(filtered_file_path, index=False)

    

    csv_filename1 = output_file_path
    csv_filename2 = filtered_file_path
    output_filename_stat = os.path.join(ROOT_DIR, params.name, f"statads_{params.name}_{params.name_csv}_{params.date_f}_{params.num_ads}.csv")
    
    txt.merge_csv_files(csv_filename1, csv_filename2, output_filename_stat)
    txt.clean_merged_data(output_filename_stat)
        


def safe_get(url, headers, retries=3, timeout=10):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.ConnectionError as e:
            print(f"[{attempt+1}/{retries}] Ошибка соединения: {e}")
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при GET-запросе: {e}")
            break
    return None

def safe_post(url, headers, payload, retries=3, timeout=10):
    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=timeout)
            response.raise_for_status() 
            return response
        except requests.exceptions.ConnectionError as e:
            print(f"[{attempt+1}/{retries}] Соединение сброшено: {e}")
            time.sleep(2)  # пауза перед повтором
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при POST запросе: {e}")
            break  # или continue, если хочешь повторить при других ошибках
    return None  # если все попытки не удались

