import pandas as pd
import requests
import time
import datetime
import os
import json

ROOT_DIR = 'C:/proj'
name = "svai"
name_csv = "srg"


print("Загрузка - Файл api.json")
# Задаем путь к файлу
api_json_path = f"{ROOT_DIR}/{name}/var/api.json"

print(api_json_path)

# Проверяем существование файла
if os.path.isfile(api_json_path):
    with open(api_json_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    # Получаем данные по ключу params.name_csv
    if name_csv in data:
        CLIENT_ID = data[name_csv]["CLIENT_ID"]
        CLIENT_SECRET = data[name_csv]["CLIENT_SECRET"]
        USER_ID = data[name_csv]["USER_ID"]

        TOKEN_URL = "https://api.avito.ru/token"
        

        print("✅ Данные api загружены успешно.")
    else:
        print(f"❌ Ключ '{name_csv}' не найден в файле.")
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


# access_token = 'ТВОЙ_ТОКЕН'  # замени на реальный токен

input_file = f"{ROOT_DIR}/{name}/stat/now_stat.xlsx"
output_file = f"{ROOT_DIR}/{name}/stat/stat_with_id.xlsx"

def get_ad_ids_by_avito_ids(access_token, avito_id_list):
    headers = {"Authorization": f"Bearer {access_token}"}
    result = {}

    for i in range(0, len(avito_id_list), 200):
        batch = avito_id_list[i:i + 200]
        query = "|".join(map(str, batch))
        url = f"https://api.avito.ru/autoload/v2/items/ad_ids?query={query}"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            items = response.json().get("items", [])
            for item in items:
                avito_id = item.get("avito_id")
                ad_id = item.get("ad_id")
                if avito_id and ad_id:
                    print(f"✅ AvitoId: {avito_id} → Id: {ad_id}")
                    result[str(avito_id)] = ad_id
                else:
                    print(f"⚠️ AvitoId: {avito_id} — не удалось получить ad_id")
        else:
            print(f"❌ Ошибка {response.status_code}: {response.text}")
            continue

        time.sleep(0.3)  # задержка, чтобы не спамить API

    return result

def main():
    # Загрузка Excel
    if not os.path.exists(input_file):
        print(f"❌ Файл не найден: {input_file}")
        return

    df = pd.read_excel(input_file)

    if 'Номер объявления' not in df.columns:
        print("❌ В файле нет столбца 'AvitoId'")
        return

    # Удаляем пустые значения
    avito_ids = df['Номер объявления'].dropna().astype(str).unique().tolist()
    print(f"🔍 Получено AvitoId: {len(avito_ids)}")

    # Получаем соответствие AvitoId → Id
    id_mapping = get_ad_ids_by_avito_ids(access_token, avito_ids)

    # Создаём столбец Id
    df['Id'] = df['Номер объявления'].astype(str).map(id_mapping)

    # Сохраняем в новый файл
    try:
        df.to_excel(output_file, index=False)
        print(f"✅ Сохранено: {output_file}")
    except Exception as e:
        print(f"❌ Ошибка при сохранении файла: {e}")

    print("✅ Готово.")

if __name__ == "__main__":
    start_time = time.time()
    main()
    duration = time.time() - start_time
    print(f"⏱️ Время выполнения: {duration:.2f} секунд")
    print(f"🕒 Время: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
