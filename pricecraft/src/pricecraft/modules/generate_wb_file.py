#!/usr/bin/env python3
# src/pricecraft/modules/generate_wb_file.py
import pandas as pd
import os, json, time, traceback
from datetime import datetime
import sys

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

# guid = 72304825 # guid листа в ссылке (когда открыт лиcт)

# from config.sheets import get_client, get_sheet

def write_log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{ts} | {msg}\n")
    print(msg)  # попадёт в nohup log

def write_status(status, message='', module='generate_wb_file'):
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
    write_status("Run")
    try:
        client = sheets.get_client()
        sh = client.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet("generate_wb_file")
        except Exception:
            ws = sh.add_worksheet(title="generate_wb_file", rows=100, cols=10)
        
        ws.clear()
        time.sleep(0.5) 
            
        # wsconf = sh.worksheet("configurations")
        wsconf = sh.worksheet("configurations")
        
        
        wsprices = sh.worksheet("Price_new")
        
        values = settings.get_values(wsprices)

        # values = {
        #     "sborka": wsprices.acell("J8").value,
        #     "proch": wsprices.acell("J9").value,
        #     "avito": wsprices.acell("B2").value,
        #     "fixavito": wsprices.acell("C2").value,
        #     "ozon": wsprices.acell("D2").value,
        #     "wb": wsprices.acell("E2").value
        # }
        time.sleep(0.5) 
        
        write_log("values wb")
        write_log(values["wb"])
        write_log("values end")

        data = wsconf.get_all_values()

        wsmapping = sh.worksheet("parameter_mapping")
        mapping_records = wsmapping.get_all_records()
        df_mapping = pd.DataFrame(mapping_records)


        df = get_config.init_conf_df(pd.DataFrame(data))

        # print(df)

        # таблица где есть цены на комплектующие и какие комплектующие идут в вариации
        df_price_kompl = get_config.down_respons(url_batya,406620284,"price_kompl.csv")
        write_log("df_price_kompl end")
        # Удаляем строки с пустыми значениями в столбце 'ОЗОН. Вариации.'
        df_price_kompl = df_price_kompl.dropna(subset=['ОЗОН. Вариации.'])


        df_final = get_config.create_wb_table(df,df_price_kompl,values,df_mapping)

    
        params = {
            "name": "sborpk",
            "name_csv": "pk",
            "num_ads": 5000,
            "address_to_append": "https://mnogunik.ru/outfile",
        }

        # import requests


        # params = {
        #     "name": "sborpk",
        #     "name_csv": "pk",
        #     "num_ads": 5000,
        #     "address_to_append": "https://mnogunik.ru/outfile",
        # }

        # payload = {
        #     "params": params
        # }

        # response = requests.post(
        #     "http://127.0.0.1:8000/run",
        #     json=payload,
        #     timeout=30
        # )

        # response.raise_for_status()

        # result = response.json()
        # print(result)

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
            ws.update('A1', rows, value_input_option="USER_ENTERED")
            ws.update(
                range_name='A1',
                values=rows,
                value_input_option="USER_ENTERED"
            )
            
            
            
            print("Таблица generate_wb_file успешно загружена (включая строки).")
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
        write_log(f"[generate_wb_file] Выполнено в {datetime.now()}")

        get_config.finish("generate_wb_file")

    
    except Exception as e:
        tb = traceback.format_exc()
        write_log("ERROR: " + str(e))
        write_log(tb)
        write_status("error", str(e), "generate_wb_file")
    
    



if __name__ == "__main__":
    main()
