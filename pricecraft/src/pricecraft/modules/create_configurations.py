#!/usr/bin/env python3
# src/pricecraft/modules/generate_config.py
import pandas as pd
import os, json, time, traceback
from datetime import datetime
import sys
# установить PROJECT_ROOT = .../src/pricecraft

# Путь для импорта test_script (если он рядом)
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
    
from pathlib import Path
    

from config import sheets
from config import settings

BATCH_SIZE = settings.BATCH_SIZE
 
 
PROJECT_ROOT = settings.PROJECT_ROOT

SPREADSHEET_ID = settings.SPREADSHEET_ID

 
# Путь к проекту (сконфигурируй при необходимости)
# PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNNERS_DIR = os.path.join(PROJECT_ROOT, 'runners')
LOG_FILE = os.path.join(RUNNERS_DIR, 'log.txt')
STATUS_FILE = os.path.join(RUNNERS_DIR, 'status.json')

# import importlib



# SPREADSHEET_ID = "1fLrruYkw0JOOszb6q4bUpNYImflxFkdem57pQPc0qnQ"  # твой id
url_batya = settings.url_batya

strok = 14 # на это строке размещаются строчные названия
stolb = 5 # в этом столбце размещаются столбчатые названия
beg_table = [8, 15]

# 140246863 старый guid листа 
guid = 72304825 # guid листа в ссылке (когда открыт лиcт)


def write_log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{ts} | {msg}\n")
    print(msg)  # попадёт в nohup log

def write_status(status, message='', module='generate_config'):
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
        write_log("START generate_config")
        write_status("running", "Создаются конфигурации...", "generate_config")
        
        
        client = sheets.get_client()        # config/sheets.get_client()
        print("client end")
        write_log("client end")
        sh = client.open_by_key(SPREADSHEET_ID)
        # лист configurations (создаст, если не существует)
        print("sh end")
        write_log("sh end")
        
        try:
            ws = sh.worksheet("configurations")
        except Exception:
            ws = sh.add_worksheet(title="configurations", rows=100, cols=10)
        # Запишем демонстрационную строку
        print("ws end")
        write_log("ws end")
        
        
        wsprices = sh.worksheet("Price_new")
        print("wsprices end")
        write_log("wsprices end")
        
        # 72304825
        ws.clear()
        print("clear end")
        write_log("clear end")
 
        
        df = get_config.down_respons_main(url_batya,guid,"cpu_gpu.xlsx", strok, stolb, "00FFFF")
        write_log("df end")

        # таблица где есть понятие какой БП подходит для ВК
        df_vk_bp = get_config.down_respons(url_batya,634015161,"vk_bp.csv")

        write_log("df_vk_bp end")

        # таблица где есть цены на комплектующие и какие комплектующие идут в вариации
        df_price_kompl = get_config.down_respons(url_batya,406620284,"price_kompl.csv")
        write_log("df_price_kompl end")
        
        # Удаляем строки с пустыми значениями в столбце 'ОЗОН. Вариации.'
        df_price_kompl = df_price_kompl.dropna(subset=['ОЗОН. Вариации.'])
        write_log("ОЗОН. Вариации end")
        

        

        # df_del = down_respons_main(url_batya,140246863,"cpu_gpu.xlsx", strok, stolb, "FF0000")
        # df_add = down_respons_main(url_batya,140246863,"cpu_gpu.xlsx", strok, stolb, "00FF00")

        # приводит таблицу к нужному виду (вид данных столбцы и строки и т п)
        df= get_config.init_df(df)
        write_log("get_config.init_df(df) end")
        
        # df_del = init_df(df_del)
        # df_add = init_df(df_add)
        
        #создаем папку если ее нет. потом нах удалить.
        os.makedirs("csv", exist_ok=True)
        
        # создаем список конфигураций где пары проц и вк
        df_blu_out = get_config.getHashTable(df)
        write_log("df_blu_out.init_df(df) end")
        
        print(f"df_blu_out first Строк: {df_blu_out.shape[0]}, столбцов: {df_blu_out.shape[1]}")


        values = settings.get_values(wsprices)
        # values = {
        #     "sborka": wsprices.acell("K8").value,
        #     "proch": wsprices.acell("K9").value,
        #     "avito": wsprices.acell("C2").value,
        #     "fixavito": wsprices.acell("D2").value,
        #     "ozon": wsprices.acell("E2").value
        # }
        write_log("values end")
        
        # value["ozon"]



        df_blu_out_final = get_config.create_table(df_blu_out,df_vk_bp,df_price_kompl,values)
        # df_blu_out_final = df_blu_out
        write_log("df_blu_out_final end")
        
        # # try:
           
        # #     # get_config = importlib.import_module('get_config')
        # #     get_config.main()
        # # except Exception as e:
        # #     print("Ошибка при выполнении get_config:", e)
            
        # Заглушка вместо df_blu_out_final
        # df_blu_out_final = pd.DataFrame({
        #     'name': ['item1', 'item2', 'item3'],
        #     'price': [100, 200, 300],
        #     'category': ['tools', 'electronics', 'furniture']
        # })
            
        # допустим final_table уже готов
        df_final = df_blu_out_final.copy()

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
        
        
 
        
        # 5) Очистить лист и записать — используем явный A1 диапазон
        
        time.sleep(0.5)  # короткая пауза, чтобы google успел обработать clear

# 2025-10-27 20:01:27 | START generate_config
# /var/www/mnogunik.ru/code/pricecraft/src/pricecraft/modules/create_configurations.py:158: DeprecationWarning: The order of arguments in worksheet.update() has changed. Please pass values first and range_name secondor used named arguments (range_name=, values=)
#   ws.update('A1', rows, value_input_option="USER_ENTERED")
# START generate_config

        # Если есть хотя бы 1 непустая строка — записываем весь блок
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

        print("--------------------------------------------------------")
        get_config.finish("create_configurations")
        print("добавлено в лог")
            

        
        # now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # msg = f"Модуль работает и в разработке. Время выполнения: {now}"
        # ws.append_row([msg])

        # # имитируем работу и промежуточные логи/статусы
        # for i in range(5):
        #     write_log(f"step {i+1}/5 - intermediate data: var={i*10}")
        #     write_status("running", f"Шаг {i+1}/5", "generate_config")
        #     time.sleep(1)

        # write_log("FINISH generate_config")
        write_status("finished", "Конфигурации созданы", "generate_config")

    except Exception as e:
        tb = traceback.format_exc()
        write_log("ERROR: " + str(e))
        write_log(tb)
        write_status("error", str(e), "generate_config")

if __name__ == "__main__":
    main()
