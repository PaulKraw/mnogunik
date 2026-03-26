#!/usr/bin/env python3
# src/pricecraft/modules/generate_ozon_file.py
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

def write_status(status, message='', module='update_wb_prices'):
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
            ws = sh.worksheet("update_wb_prices")
        except Exception:
            ws = sh.add_worksheet(title="update_wb_prices", rows=100, cols=10)
        
        ws.clear()
        time.sleep(0.5) 
            
        # wsconf = sh.worksheet("configurations")
        wsconf = sh.worksheet("configurations")
        
        
        # sheet = sheets.get_sheet(client, 'generate_ozon_file')  # одноимённый лист
        # sheet.clear()
        # ws.update('A1', f"Модуль работает и в разработке. Время выполнения: {datetime.datetime.now()}")

        # df = pd.DataFrame(wsconf)
        data = wsconf.get_all_values()
        df = pd.DataFrame(data)

        # Сохраняем первую строку заголовков
        headers = df.iloc[0].tolist()
        # old_headers = df.iloc[0]
        # Создаём новую первую строку с твоим текстом, остальное пустое
        first_row_text = "Данная таблица будет переделываться для ОТЧЕТА ОБНОВЫ ЦЕН НА ВБ используя артикулы и начальную цену"
        new_first_row = [first_row_text] + [""] * (len(headers) - 1)

        # Сдвигаем старую строку заголовков вниз
        # df.iloc[-1] = new_first_row
        df.columns = new_first_row
        df.loc[0] = headers  # вставляем старую строку заголовков как вторую
        df.index = df.index + 1  # сдвигаем индекс
        df = df.sort_index()  # сортируем по индексу
    
            # допустим final_table уже готов
        df_final = df.copy()

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
        
    # print(f"[update_wb_prices] Выполнено в {datetime.datetime.now()}")
        write_log(f"[update_wb_prices] Выполнено в {datetime.now()}")
    
    except Exception as e:
        tb = traceback.format_exc()
        write_log("ERROR: " + str(e))
        write_log(tb)
        write_status("error", str(e), "update_wb_prices")
    
      



if __name__ == "__main__":
    main()
