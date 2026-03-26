import pandas as pd
import requests
import time
import json
import os
from datetime import datetime
import logging
import sys
import csv  # 👈 добавил

ROOT_DIR = 'C:/proj'
REPORT_CSV = os.path.join(ROOT_DIR, "runs_report.csv")  # 👈 файл отчётов в корне

# 🔁 Список проектов
projects = [
    {"name": "svai", "name_csv": "alx_s"},
    {"name": "svai", "name_csv": "evg"},
    {"name": "svai", "name_csv": "srg"},
    {"name": "svai", "name_csv": "yur"},
    # {"name": "dearek", "name_csv": "anker"},
]

# 👉 Аккаунты
accounts = [
    ("svai", "alx_s"),
    ("svai", "evg"),
    ("svai", "srg"),
    ("svai", "yur"),
    ("svai", "rmn"),
    # ("stroy", "str"),
    # ("priv", "pch"),
    # ("dearek", "anker")
]

# name = "svai"; name_csv = "srg"
name = "svai"
name_csv = "srg"

input_file = f"{ROOT_DIR}/{name}/stat/stavki_{name_csv}.xlsx"

api_json_path = f"{ROOT_DIR}/{name}/var/api.json"

with open(api_json_path, "r", encoding="utf-8") as file:
    data = json.load(file)

def log_to_file(message, log_file):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - {message}\n")

def get_access_token(client_id, client_secret):
    response = requests.post(
        "https://api.avito.ru/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
    )
    return response.json().get("access_token")

def get_ad_ids_by_avito_ids(token, avito_ids):
    headers = {"Authorization": f"Bearer {token}"}
    result = {}
    for i in range(0, len(avito_ids), 200):
        batch = avito_ids[i:i + 200]
        query = "|".join(map(str, batch))
        url = f"https://api.avito.ru/autoload/v2/items/ad_ids?query={query}"
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            for item in r.json().get("items", []):
                if item.get("avito_id") and item.get("ad_id"):
                    result[str(item["avito_id"])] = item["ad_id"]
        else:
            print(f"❌ Ошибка при получении ad_id: {r.status_code}")
        time.sleep(0.2)
    return result

def set_manual_bid(token, ad_id, bid_penny, action_type_id=5, limit_penny=None):
    url = "https://api.avito.ru/cpxpromo/1/setManual"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "itemID": ad_id,
        "bidPenny": bid_penny,
        "actionTypeID": action_type_id
    }
    if limit_penny is not None:
        payload["limitPenny"] = limit_penny
    response = safe_post(url, headers, payload)
    if response is None:
        return 500, {"error": "No response"}
    try:
        return response.status_code, response.json()
    except Exception:
        return response.status_code, {"error": "Invalid JSON"}

def safe_post(url, headers, payload, retries=3, timeout=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=timeout)
            if response.status_code == 200:
                return response
            elif response.status_code in [429, 500, 502, 503, 504]:
                print(f"⚠️ [{attempt}/{retries}] Сервер ответил {response.status_code}, пробуем снова через {delay} сек...")
                time.sleep(delay); delay *= 2
            elif response.status_code == 403:
                print(f"🚫 [{attempt}/{retries}] Доступ запрещён (403). Ждём {delay} сек и пробуем снова...")
                time.sleep(delay); delay *= 2
            else:
                print(f"❌ [{attempt}/{retries}] Ошибка {response.status_code}: {response.text}")
                return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"🔌 [{attempt}/{retries}] Сетевая ошибка: {e}. Пауза {delay} сек...")
            time.sleep(delay); delay *= 2
        except requests.exceptions.RequestException as e:
            print(f"❗ [{attempt}/{retries}] Неизвестная ошибка запроса: {e}")
            break
    print("❌ Все попытки запроса исчерпаны. Сервер не отвечает.")
    return None

def process_account(name, name_csv, df):
    client_name = name_csv
    propusk = 0
    primenil = 0
    oshibka = 0

    api_json_path = f"{ROOT_DIR}/{name}/var/api.json"
    log_file = f"{ROOT_DIR}/{name}/stat/stavki_{name_csv}_log.txt"

    if not os.path.exists(api_json_path):
        print(f"❌ Нет файла API: {api_json_path}")
        return propusk, primenil, oshibka, df

    with open(api_json_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    if client_name not in data:
        print(f"❌ Не найден клиент '{client_name}' в {api_json_path}")
        return propusk, primenil, oshibka, df

    client_id = data[name_csv]["CLIENT_ID"]
    client_secret = data[name_csv]["CLIENT_SECRET"]
    access_token = get_access_token(client_id, client_secret)

    for index, row in df.iterrows():
        if str(row.get('!Применил')).strip().lower() == 'да':
            propusk += 1
            continue
        if pd.isna(row.get("AvitoId")) or str(row.get("AvitoId")).strip().lower() in ["", "none"]:
            print(f"⏭️ Пропускаем {index} — нет авитоайди.")
            continue

        avito_id = str(int(row["AvitoId"]))
        bid_rub = row["Ставка"]
        bid_penny = int(float(bid_rub) * 100)
        ad_id = int(avito_id)

        limit_penny_rub = row["Лимит"]
        limit_penny = int(float(limit_penny_rub) * 100)

        status_code, response = set_manual_bid(access_token, ad_id, bid_penny, action_type_id=5, limit_penny=limit_penny)

        if status_code == 200:
            sys.stdout.write(f"\r Применено ставок : {primenil+1} - {name}-{name_csv}")
            sys.stdout.flush()
            df.at[index, '!Применил'] = 'да'
            primenil += 1
        else:
            print(f"❌ Ошибка для {ad_id} ({status_code}): {response.get('message')}")
            log_to_file(f"{ad_id} - ошибка {status_code}: {response}", log_file)
            oshibka += 1
        time.sleep(0.2)

    print(f"\n 💾 Обновлено: {name}/{name_csv} — всего: {primenil+propusk+oshibka}; ")
    print(f"\n применил: {primenil}; пропустил: {propusk}; ошибок: {oshibka}; ")

    return propusk, primenil, oshibka, df

def format_duration(seconds: float) -> str:
    """HH:MM:SS для красивого вывода."""
    s = int(seconds)
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

# Основная функция
def main():
    # ⏱️ старт замера
    start_time = time.time()
    session_applied = 0
    session_errors = 0
    session_skipped = 0

    input_file = "stavki_all.xlsx"
    df = pd.read_excel(input_file)

    required_columns = {"name", "name_csv", "AvitoId", "Ставка"}
    if not required_columns.issubset(df.columns):
        print(f"❌ Файл должен содержать столбцы: {', '.join(required_columns)}")
        print(", ".join(df.columns))
        return
    
    if '!Применил' not in df.columns:
        df['!Применил'] = ''

    while True:
        total_propusk = 0
        total_primenil = 0
        total_oshibka = 0

        grouped = df.groupby(["name", "name_csv"])

        for (name, name_csv), df_client in grouped:
            print(f"\n🚀 Обработка: {name}/{name_csv}")

            df_client_copy = df_client.copy()
            propusk, primenil, oshibka, df_client_updated = process_account(
                name, name_csv, df_client_copy
            )

            index_mask = (df["name"] == name) & (df["name_csv"] == name_csv)
            df.loc[index_mask, "!Применил"] = df_client_updated["!Применил"]

            total_propusk += propusk
            total_primenil += primenil
            total_oshibka += oshibka

        # накапливаем по проходам (если будет повторный)
        session_applied += total_primenil
        session_errors += total_oshibka
        session_skipped += total_propusk

        print("\n📊 Общая статистика:")
        print(f"Применено: {total_primenil}")
        print(f"Пропущено: {total_propusk}")
        print(f"Ошибок: {total_oshibka}")
        
        if total_primenil > 0:
            print("\n🔁 Повторный проход, так как были изменения...")
            time.sleep(1)
        else:
            print("\n🏁 Всё применено. Завершаем.")
            break

    df.to_excel(input_file, index=False)
    print(f"\n📥 Обновлённый файл сохранён: {input_file}")

    # ⏱️ финиш и вывод времени
    elapsed_seconds = time.time() - start_time
    elapsed_minutes = elapsed_seconds / 60.0
    processed_ads = session_applied
    avg_min_per_ad = (elapsed_minutes / processed_ads) if processed_ads > 0 else 0.0

    print(f"\n⏱️ Время выполнения: {format_duration(elapsed_seconds)} ({elapsed_seconds:.2f} сек)")
    print(f"📈 Всего применено: {processed_ads}, ошибок: {session_errors}, пропущено: {session_skipped}")
    print(f"🧮 Минут на объявление: {avg_min_per_ad:.4f}")

    # 📝 запись отчёта в CSV (append, с заголовком при первом создании)
    os.makedirs(ROOT_DIR, exist_ok=True)
    header = ["datetime", "applied", "errors", "duration_seconds", "duration_minutes", "minutes_per_ad"]
    row = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        processed_ads,
        session_errors,
        round(elapsed_seconds, 2),
        round(elapsed_minutes, 2),
        round(avg_min_per_ad, 4)
    ]
    need_header = not os.path.exists(REPORT_CSV)
    with open(REPORT_CSV, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=",")
        if need_header:
            writer.writerow(header)
        writer.writerow(row)
    print(f"📝 Отчёт дописан: {REPORT_CSV}")

if __name__ == "__main__":
    main()
