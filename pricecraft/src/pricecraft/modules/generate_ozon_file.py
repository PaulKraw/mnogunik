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

def write_status(status, message='', module='generate_ozon_file'):
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
            ws = sh.worksheet("generate_ozon_file")
        except Exception:
            ws = sh.add_worksheet(title="generate_ozon_file", rows=100, cols=10)
        
        ws.clear()
        time.sleep(0.5) 
            
        # wsconf = sh.worksheet("configurations")
        wsconf = sh.worksheet("configurations")
        
        wsprices = sh.worksheet("Price_new")
        
        values = settings.get_values(wsprices)

        write_log("values ozon")
        write_log(values["ozon"])
        write_log("values end")

        # df = pd.DataFrame(wsconf)
        data = wsconf.get_all_values()
        
        

        wsmapping = sh.worksheet("parameter_mapping")
        mapping_records = wsmapping.get_all_records()
        df_mapping = pd.DataFrame(mapping_records)
        

        
        df = get_config.init_conf_df(pd.DataFrame(data))
        
        
               # таблица где есть цены на комплектующие и какие комплектующие идут в вариации
        df_price_kompl = get_config.down_respons(url_batya,406620284,"price_kompl.csv")
        write_log("df_price_kompl end")
        
        # Удаляем строки с пустыми значениями в столбце 'ОЗОН. Вариации.'
        df_price_kompl = df_price_kompl.dropna(subset=['ОЗОН. Вариации.'])
               
        
        # df_final = get_config.create_ozon_table(df,df_price_kompl,values,ozon_mapping)
        df_final = get_config.create_ozon_table(df,df_price_kompl,values,df_mapping)




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
        write_log(f"[generate_ozon_file] Выполнено в {datetime.now()}")
        get_config.finish("generate_ozon_file")

    
    except Exception as e:
        tb = traceback.format_exc()
        write_log("ERROR: " + str(e))
        write_log(tb)
        write_status("error", str(e), "generate_ozon_file")
    
        # for name_table in list_par_kom:
        #     print(f"обработка комлл - {name_table}")
        # final_table = merge_oz_param(final_table, name_table)
        
    # for i, row in final_table.iterrows():
        
        
        
    #     cp_list = [
    #         f"{row['Число ядер процессора']}х{row['Частота процессора, ГГц']} ГГц",
    #         "",
    #     ]

    #     # Случайный выбор значений
    #     title_type = random.choice(my_list)
    #     cpu_info = random.choice(cp_list)

    #     # Формирование названий
    #     name_conf = (
    #         f"{title_type} ULTRAFPS ({row['Видеокарта*']}, {row['Процессор*']} {cpu_info}, "
    #         f"{row['Оперативная память*']}, {row['Диск ГБ']} ГБ, {row['Кейс название']})"
    #     )

    #     name_conf2 = (
    #         f"{title_type} {row['Кейс название']} ULTRAFPS ({row['Видеокарта*']}, "
    #         f"{row['Процессор*']} {cpu_info}, {row['Оперативная память*']}, {row['Диск ГБ']} ГБ)"
    #     )

    #     # Запись случайного варианта в столбец
    #     final_table.loc[i, "Title"] = random.choice([name_conf, name_conf2])
        
        
        
        
    #     final_table['imgpar'] = "img.json"
    #     final_table['Id'] = ""
    #     final_table['DateBegin'] = ""
    #     final_table['Address'] = ""
    #     final_table['Город'] = "все"
    #     final_table['Description'] = "text_ozon.txt"
    #     final_table['images_folder'] = "feat"
    #     final_table['count_img'] = "1"
    #     final_table['first_img'] = "img_first_pk_ozon.json"
    #     final_table['Price'] = row["Сумма"]
    #     final_table['ImageUrls'] = ""
    #     final_table['Brand'] = "Другой"
    #     final_table['Type'] = "Игровой | Офисный"
        
        



if __name__ == "__main__":
    main()
