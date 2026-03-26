#!/usr/bin/env python3
# src/pricecraft/modules/generate_ozon_file.py
import pandas as pd
import os, json, time, traceback
from datetime import datetime
import sys
import requests


# Путь для импорта test_script (если он рядом)
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
    
from config import sheets
from config import settings
PROJECT_ROOT = settings.PROJECT_ROOT
SPREADSHEET_ID = settings.SPREADSHEET_ID

# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNNERS_DIR = os.path.join(PROJECT_ROOT, 'runners')
LOG_FILE = os.path.join(RUNNERS_DIR, 'log.txt')
STATUS_FILE = os.path.join(RUNNERS_DIR, 'status.json')

url_batya = settings.url_batya


API_URL = "https://api-seller.ozon.ru/v1/product/import/prices"

HEADERS_combat = {
    "Client-Id": "1456803",  # Замени на свой Client-Id
    "Api-Key": "29a88933-3390-4bed-aaec-1220052c268d",  # Замени на свой API-ключ
    "Content-Type": "application/json"
}
HEADERS = HEADERS_combat


# guid = 72304825 # guid листа в ссылке (когда открыт лиcт)

# from config.sheets import get_client, get_sheet

def write_log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{ts} | {msg}\n")
    print(msg)  # попадёт в nohup log

def write_status(status, message='', module='update_ozon_prices'):
    data = {
        "module": module,
        "status": status,   # running | idle | error | finished
        "message": message,
        "timestamp": datetime.utcnow().isoformat() + 'Z'
    }
    with open(STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

import get_config 




def main():
    try:
        client = sheets.get_client()
        sh = client.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet("update_ozon_prices")
        except Exception:
            ws = sh.add_worksheet(title="update_ozon_prices", rows=100, cols=10)
        
        ws.clear()
        time.sleep(0.5) 
            
        # wsconf = sh.worksheet("configurations")
        wsconf = sh.worksheet("generate_ozon_file")
        
        
        # sheet = sheets.get_sheet(client, 'generate_ozon_file')  # одноимённый лист
        # sheet.clear()
        # ws.update('A1', f"Модуль работает и в разработке. Время выполнения: {datetime.datetime.now()}")

        # df = pd.DataFrame(wsconf)
        data = wsconf.get_all_values()
        df = get_config.init_conf_df(pd.DataFrame(data))
        

        
        df = df[["Артикул*", "Название товара", "Цена, руб.*"]]



        prices = {}
        
        # Формируем словарь articul → цена
        for _, row in df.iterrows():
            articul = str(row["Артикул*"]).strip()
            try:
                new_price = float(str(row["Цена, руб.*"]).replace(",", "."))
                prices[articul] = new_price
            except ValueError:
                print(f"❌ Некорректная цена у артикула {articul}: {row['Цена, руб.*']}")

        price_updates = []

        for articul in prices:
            
            new_price = prices[articul]
            old_price = round(new_price * 1.11, 2)
            min_price = round(new_price * 0.97, 2)
            price_updates.append({
                "auto_action_enabled": "DISABLED",
                "auto_add_to_ozon_actions_list_enabled": "DISABLED",
                "currency_code": "RUB",
                "min_price": str(round(min_price)),
                "min_price_for_auto_actions_enabled": True,
                "net_price": "",
                "offer_id": articul,
                "old_price": str(round(old_price)),
                "price": str(round(new_price)),
                "price_strategy_enabled": "DISABLED",
                "quant_size": 1,
                "vat": "0"
            })


        BATCH_SIZE = 950  # Размер пачки
        DELAY = 2  # Задержка между запросами в секундах
        
        # Функция для отправки батчами
        def send_price_updates(price_updates):
            total_batches = (len(price_updates) + BATCH_SIZE - 1) // BATCH_SIZE
            report_rows = []  # для отчета
            
            for i in range(0, len(price_updates), BATCH_SIZE):
                batch = price_updates[i:i + BATCH_SIZE]
                payload = {"prices": batch}

                response = requests.post(API_URL, headers=HEADERS, json=payload, verify=False)

                if response.status_code == 200:
                    print(f"✅ Пакет {i//BATCH_SIZE + 1}/{total_batches} успешно загружен ({len(batch)} записей).")
                    for item in batch:
                        report_rows.append({
                        "offer_id": item["offer_id"],
                        "old_price": item["old_price"],
                        "new_price": item["price"],
                        "status": "updated"
                    })
                else:
                    print(f"❌ Ошибка при загрузке пакета {i//BATCH_SIZE + 1}: {response.text}")
                    for item in batch:
                        report_rows.append({
                            "offer_id": item["offer_id"],
                            "old_price": item["old_price"],
                            "new_price": item["price"],
                            "status": "error"
                        })

                time.sleep(DELAY)
                
            return report_rows

        if price_updates:
            report_rows = send_price_updates(price_updates)
            print("Конец работы.")
        else:
            print("⚠️ Нет данных для обновления.")


        report_df = pd.DataFrame(report_rows)
        df_price_updates = pd.DataFrame(price_updates)
        # Совмещаем с df_final по offer_id
        df_final = df_price_updates.merge(report_df, on="offer_id", how="left")
        
            # допустим final_table уже готов
        

        # 1) безопасно очистим inf/NaN и приведём к native
        # df_final = df_final.replace([np.inf, -np.inf], np.nan)
        df_final = df_final.fillna("")          # безопасно для NaN
        df_final = df_final.applymap(lambda x: x.item() if hasattr(x, "item") else x)
        df_final = df_final.reset_index(drop=True)
               
        
        # 3) Формируем rows: заголовок + все строки
        rows = [df_final.columns.tolist()] + df_final.astype(str).values.tolist()

        # 4) Ещё быстрая проверка: есть ли хотя бы одна непустая ячейка в каждой строке
        non_empty_per_row = [any(cell.strip() for cell in r) for r in rows[1:]]
        print(">>> non-empty rows count:", sum(non_empty_per_row), "of", len(non_empty_per_row))
        
        
    
        if len(df_final) > 0 and sum(non_empty_per_row) > 0:
                # Убедимся, что rows содержит корректную вложенность list[list[str]]
            # ws.update('A1', rows, value_input_option="USER_ENTERED")
            ws.update(
                range_name='A1',
                values=rows,
                value_input_option="USER_ENTERED"
            )
            
            
            
            print("Таблица успешно загружена (включая строки).")
        else:
            # Если строки есть, но все пустые — всё равно запишем, но добавим маркеры
            if len(df_final) > 0:
                # добавим индикатор в первую колонку каждой строки, чтобы гугл создал строки
                rows_with_marker = [rows[0]] + [[("!empty!" if i == 0 else "")] + r[1:] for i, r in enumerate(rows[1:], start=1)]
                # ws.update('A1', rows_with_marker, value_input_option="USER_ENTERED")
                ws.update(
                    range_name='A1',
                    values=rows_with_marker,
                    value_input_option="USER_ENTERED"
                )
                print("Записаны строки, которые были полностью пустыми (поставлен маркер в первой колонке).")
            else:
                print("DataFrame пуст — записаны только заголовки (ожидаемо).")
        
    # print(f"[generate_ozon_file] Выполнено в {datetime.datetime.now()}")
        write_log(f"[update_ozon_prices] Выполнено в {datetime.now()}")
        get_config.finish("update_ozon_prices")
        
    
    except Exception as e:
        tb = traceback.format_exc()
        write_log("ERROR: " + str(e))
        write_log(tb)
        write_status("error", str(e), "update_ozon_prices")
    

        



if __name__ == "__main__":
    main()
