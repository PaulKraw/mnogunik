#!/usr/bin/env python3
# src/pricecraft/modules/generate_ozon_file.py
import pandas as pd
import os, json, time, traceback
from datetime import datetime
import sys
import requests
import subprocess
from pathlib import Path
from google.auth.exceptions import RefreshError

# Путь для импорта test_script (если он рядом)
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
    
from config import sheets
from config import settings
PROJECT_ROOT = settings.PROJECT_ROOT
SPREADSHEET_ID = settings.SPREADSHEET_ID
SPREADSHEET_CLROWS_ID = settings.SPREADSHEET_CLROWS_ID

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

def write_status(status, message='', module='generate_ozon_content'):
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
            ws = sh.worksheet("content")
        except Exception:
            ws = sh.add_worksheet(title="content", rows=100, cols=10)
        time.sleep(0.5) 
        
        # ws.clear()
        # time.sleep(0.5) 
            
        # wsconf = sh.worksheet("configurations")
        wsconf = sh.worksheet("configurations")
        time.sleep(0.5) 
        
        wsprices = sh.worksheet("Price_new")
        
        values = settings.get_values(wsprices)
        time.sleep(0.5) 

        write_log("values ozon")
        write_log(values["ozon"])
        write_log("values end")

        # df = pd.DataFrame(wsconf)
        data = wsconf.get_all_values()
        

        # # новый маппинг параметров 

        wsmapping = sh.worksheet("parameter_mapping")
        mapping_records = wsmapping.get_all_records()
        df_mapping = pd.DataFrame(mapping_records)
        
   
        
        df = get_config.init_conf_df(pd.DataFrame(data))
        
        marketplaces = ["ozon", "ozon_cont"]

        for marketplace in marketplaces:
            df = get_config.apply_marketplace_mapping(
                df_out=df,
                df_mapping=df_mapping,
                marketplace=marketplace
            )
            
        #marketplace получается последний из списка


        # Подготавливаем данные
        headers = df.columns.tolist()
        
        # Конвертируем все значения в строки и заменяем NaN
        data_to_write = [headers]
        for _, row in df.iterrows():
            row_data = []
            for col in headers:
                value = row[col] if col in row else ''
                if pd.isna(value):
                    value = ''
                row_data.append(str(value))
            data_to_write.append(row_data)
        
        # Записываем
        ws.clear()
        time.sleep(1)
        ws.update('A1', data_to_write)
        
        write_log(f"Записано {len(df)} строк в лист content")
        # write_log(f"=== СТАТИСТИКА ===")
        # write_log(f"Артикулов в файле: {len(file_data_dict)}")
        # write_log(f"Артикулов в content: {len(df)}")

        # за прайс для генерации используется лист с конфигурациями
        # wsconf = sh.worksheet("configurations")
        # создаем ссылку на лист с конфигами по типу https://docs.google.com/spreadsheets/d/18XoFtoLE3askqCfKWTf4U1pmnJs-r22QlRKD_BfjydM/edit?gid=401278994#gid=401278994
                # 5. Создаем ссылку на лист с конфигами
        # Формат: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit?gid={GID}
        # Нужно получить gid листа configurations
        try:
            # Получаем список всех листов и находим configurations
            sheet_list = sh.worksheets()
            config_gid = None
            for sheet in sheet_list:
                if sheet.title == "content":
                    config_gid = sheet.id
                    break
            time.sleep(0.5) 
            
            if config_gid:
                file_price_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit?gid={config_gid}"
                write_log(f"Ссылка на configurations: {file_price_url}")
            else:
                file_price_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit"
                write_log("Не найден gid для content")
        except Exception as e:
            write_log(f"Ошибка получения gid: {e}")
            file_price_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit"
        
        time.sleep(0.5) 
        # 6. Открываем лист с задачами на генерацию (предполагаем, что он называется "tasks")

        sh_clrows = client.open_by_key(SPREADSHEET_CLROWS_ID)
        try:
            ws_clrows = sh_clrows.worksheet("Лист1")
        except Exception:
            ws_clrows = sh_clrows.add_worksheet(title="Лист1", rows=1000, cols=20)
            headers = ["id", "run", "file_price", "name_csv", "name", "m", "num_ads", "date_f", "address_to_append", "status", "created_at"]
            ws_clrows.append_row(headers)
            time.sleep(1)

        time.sleep(0.5) 
        # 7. Получаем существующие задачи и вычисляем следующий id
        try:
            tasks_data = ws_clrows.get_all_records()
            if tasks_data:
                # Находим максимальный id
                max_id = max(int(task.get('m', 0)) for task in tasks_data if task.get('m'))
                next_id = max_id + 1
            else:
                next_id = 1
        except Exception as e:
            write_log(f"Ошибка чтения задач: {e}")
            next_id = 1

        # открывает лист с задачами на генерация
        time.sleep(0.5) 
        # 8. Обновляем все существующие задачи: ставим active=0
        try:
            if tasks_data:
                write_log(f"Найдено {len(tasks_data)} задач, обновляю run=0...")
                
                # Получаем ВСЕ данные листа (не только records)
                all_data = ws_clrows.get_all_values()  # 1 read запрос
                time.sleep(1)
                
                # Изменяем данные локально
                for i in range(1, len(all_data)):  # пропускаем заголовок (строка 0)
                    if i < len(all_data):
                        # Второй столбец (индекс 1) - это 'run'
                        all_data[i][1] = '0'  # устанавливаем run=0
                
                # Загружаем ВСЕ данные обратно - 1 write запрос
                ws_clrows.update('A1', all_data)
                time.sleep(1)
                write_log("Все задачи обновлены (run=0)")
                
        except Exception as e:
            write_log(f"Ошибка обновления задач: {e}")
        
        

                
        # вводит новую задачу, ставит всем нули, оставляя последней 1.
        # в поле "file_price" вводим ссылку на лист с конфигурациями. 
        # в поле "name_csv" ставим имя из переменной marketplace,
        # "name" = sborpk, 
        # "m" - следующее число которое следует(там числовой порядок)
        # "num_ads" - количество полей из таблицы с конфгурациями 
        # "date_f" дата сегодня в формате 2025-12-29 
        # "address_to_append" - https://mnogunik.ru/outfile/
        # 9. Создаем новую задачу
        name = "sborpk"
        num_ads = len(df)  # количество строк в конфигурациях
        date_f = datetime.now().strftime("%Y-%m-%d")
        
        new_task = [
            next_id,                    # id
            1,                          # active (последняя активная)
            "",
            name,                       # name
            marketplace,                # name_csv
            num_ads,                    # num_ads
            file_price_url,             # file_price
            date_f,                     # date_f
            "https://mnogunik.ru/outfile/"  # address_to_append
        ]
        time.sleep(0.5) 
        ws_clrows.append_row(new_task)
        write_log(f"Создана новая задача ID: {next_id}")
        



        # запускает задачу то есть запускает скрипт генерации (как то надо запустить файл пайтон зная только где находится запускаемый файл(в целом там уже настроено так что он запускается с другого фронта))
        # ожидает(проверять каждые 90сек) появление файла, по таймингу если через час нет файла то выдает ошибку и завершает скрипт
        # ссылка на файл будет формата "mnogunik.ru/proj/sborpk/sborpk_"{marketplace}"_"{date_f}"_"{num_ads}".csv"

        # если файл появился то он считывает информацию с файла загружает в свои поля согласно айди "ozon img" "ozon text"
# 10. Запускаем PHP скрипт генерации через HTTP
        php_script_url = "https://mnogunik.ru/mnogunik/run.php"
        
        # Параметры для запуска (предполагаем, что run.php ожидает параметры)
        params = {
            'key': 'super123Lisa'
        }
        
        try:
            write_log(f"Запуск PHP скрипта: {php_script_url}")
            
            # Отправляем GET запрос к PHP скрипту
            response = requests.get(
                php_script_url, 
                params=params,
                timeout=10  # таймаут на запуск
            )
            
            if response.status_code == 200:
                write_log(f"PHP скрипт запущен успешно. Ответ: {response.text[:100]}")
            else:
                write_log(f"Ошибка запуска PHP скрипта. Код: {response.status_code}, Ответ: {response.text}")
                ws_clrows.update_cell(len(ws_clrows.get_all_values()), 10, f"error: HTTP {response.status_code}")
                return
                
        except requests.RequestException as e:
            error_msg = f"Ошибка подключения к PHP скрипту: {str(e)}"
            write_log(error_msg)
            ws_clrows.update_cell(len(ws_clrows.get_all_values()), 10, error_msg)
            return

 # 11. Ожидаем появление файла
        expected_filename = f"{name}_{marketplace}_{date_f}_{num_ads}.csv"
        file_url = f"https://mnogunik.ru/proj/{name}/{expected_filename}"
        
        # Также проверяем в outfile директории
        file_url_outfile = f"https://mnogunik.ru/outfile/{expected_filename}"
        
        write_log(f"Ожидаем файл по URL: {file_url}")
        write_log(f"Или по URL: {file_url_outfile}")
        
        max_wait_time = 3600  # 1 час
        check_interval = 240   # 90 секунд
        start_time = time.time()
        file_found = False
        final_file_url = None
        
        while time.time() - start_time < max_wait_time:
            try:
                # Проверяем оба возможных расположения файла
                for check_url in [file_url, file_url_outfile]:
                    response = requests.head(check_url, timeout=10)
                    if response.status_code == 200:
                        file_found = True
                        final_file_url = check_url
                        write_log(f"Файл найден по URL: {check_url}")
                        break
                
                if file_found:
                    break
                    
            except requests.RequestException:
                pass
            except Exception as e:
                write_log(f"Ошибка проверки файла: {e}")
            
            # Ждем перед следующей проверкой
            wait_msg = f"Файл еще не создан, ждем {check_interval} секунд... (прошло {int(time.time() - start_time)} сек.)"
            write_log(wait_msg)
            time.sleep(check_interval)
        
        # 12. Обработка результата ожидания
        if not file_found:
            error_msg = "Файл не создан в течение часа"
            write_log(error_msg)
            ws_clrows.update_cell(len(ws_clrows.get_all_values()), 10, error_msg)
            return
        
        # 13. Файл найден - считываем информацию
        try:
            # Скачиваем файл
            # response = requests.get(final_file_url, timeout=30)
            # df_result = pd.read_csv(pd.compat.StringIO(response.text), encoding='utf-8-sig')
            df_result = pd.read_csv(final_file_url, dtype=str)
            
            write_log(f"Файл загружен, строк: {len(df_result)}")
            write_log(f"Колонки: {df_result.columns.tolist()}")
            
            # 14. Загружаем данные в лист content
            # ws.clear()

            # 1. Получаем текущие данные из листа content
            try:
                # Выгружаем весь лист content
                content_data = ws.get_all_values()
                time.sleep(1)
                
                if content_data:
                    # Создаем DataFrame из текущих данных
                    content_headers = content_data[0]
                    content_rows = content_data[1:] if len(content_data) > 1 else []
                    
                    df_current = pd.DataFrame(content_rows, columns=content_headers)
                    write_log(f"Текущий content: {len(df_current)} строк")
                else:
                    df_current = pd.DataFrame()
                    write_log("Лист content пуст")
                    
            except Exception as e:
                write_log(f"Ошибка чтения content: {e}")
                df_current = pd.DataFrame()

            # 2. Определяем какие колонки нужно переносить из файла
            # Колонки которые нужно взять из файла и вставить в content
            SOURCE_COLS = ['Description', 'ImageUrls']  # что берем из файла
            TARGET_COLS = ['Description_ozon', 'ImageUrls_ozon']  # куда вставляем в content

            # 3. Проверяем что в файле есть колонка articul
            if 'articul' not in df_result.columns:
                write_log("ОШИБКА: В файле нет колонки 'articul'")
                return
            
            # 4. Создаем маппинг артикул -> данные из файла
            file_data_dict = {}
            for _, row in df_result.iterrows():
                articul = str(row['articul']).strip()
                if articul and articul.lower() != 'nan':
                    # Сохраняем только нужные колонки
                    file_data_dict[articul] = {
                        'Description': row.get('Description', ''),
                        'ImageUrls': row.get('ImageUrls', '')
                    }

            write_log(f"Загружено {len(file_data_dict)} артикулов из файла")
            # 5. Если content пустой, создаем его с базовой структурой
            if df_current.empty or 'articul' not in df_current.columns:
                write_log("Создаем новый content")
                
                # Создаем базовые колонки
                base_columns = ['articul', 'Description_ozon', 'ImageUrls_ozon']
                
                # Собираем данные
                rows = []
                for articul, data in file_data_dict.items():
                    row = {
                        'articul': articul,
                        'Description_ozon': data.get('Description', ''),
                        'ImageUrls_ozon': data.get('ImageUrls', '')
                    }
                    rows.append(row)
                
                df_final = pd.DataFrame(rows, columns=base_columns)
                
            else:
                # 6. Объединяем существующий content с данными из файла
                write_log("Обновляем существующий content")
                
                # Проверяем что в content есть нужные целевые колонки
                # Если нет - добавляем их
                for target_col in TARGET_COLS:
                    if target_col not in df_current.columns:
                        df_current[target_col] = ''
                
                # Создаем словарь артикул -> список индексов (если есть дубли)
                content_dict = {}
                duplicate_articuls = set()

                for idx, row in df_current.iterrows():
                    articul = str(row['articul']).strip() if 'articul' in row and pd.notna(row['articul']) else ''
                    if articul and articul.lower() != 'nan':
                        if articul in content_dict:
                            # Нашли дубль
                            content_dict[articul].append(idx)
                            duplicate_articuls.add(articul)
                        else:
                            content_dict[articul] = [idx]

                if duplicate_articuls:
                    write_log(f"ВНИМАНИЕ: Найдены дубли артикулов в content: {list(duplicate_articuls)}")

                
                # Обновляем существующие строки
                updated_count = 0
                new_count = 0

                for articul, file_data in file_data_dict.items():
                    if articul in content_dict:
                        # Артикул есть в content - обновляем ВСЕ дубли
                        indices = content_dict[articul]  # это список индексов
                        for idx in indices:
                            df_current.at[idx, 'Description_ozon'] = file_data.get('Description', '')
                            df_current.at[idx, 'ImageUrls_ozon'] = file_data.get('ImageUrls', '')
                            updated_count += 1
                        if len(indices) > 1:
                            write_log(f"Обновлено {len(indices)} дублей артикула: {articul}")
                    else:
                        # Артикула нет в content - добавляем новую строку
                        new_row = {'articul': articul}
                        for target_col, source_col in zip(TARGET_COLS, SOURCE_COLS):
                            new_row[target_col] = file_data.get(source_col, '')
                        
                        # Копируем остальные колонки из существующих строк если они есть
                        for col in df_current.columns:
                            if col not in ['articul'] + TARGET_COLS and col not in new_row:
                                new_row[col] = ''
                        
                        df_current = pd.concat([df_current, pd.DataFrame([new_row])], ignore_index=True)
                        new_count += 1
                
                # 7. Очищаем ozon поля у артикулов которых нет в файле
                cleared_count = 0
                for idx, row in df_current.iterrows():
                    articul = str(row['articul']).strip() if 'articul' in row and pd.notna(row['articul']) else ''
                    if articul and articul not in file_data_dict:
                        df_current.at[idx, 'Description_ozon'] = ''
                        df_current.at[idx, 'ImageUrls_ozon'] = ''
                        cleared_count += 1
                
                df_final = df_current
                write_log(f"Обновлено: {updated_count}, добавлено новых: {new_count}, очищено: {cleared_count}")

            # 8. Записываем в Google Sheets

            # Получаем текущий список колонок
            cols = df_final.columns.tolist()

            # Удаляем нужные колонки из списка (если они присутствуют)
            for col in ['Description_ozon', 'ImageUrls_ozon']:
                if col in cols:
                    cols.remove(col)

            # Вставляем их на позиции 1 и 2 (индексы 1 и 2)
            cols.insert(1, 'Description_ozon')
            cols.insert(2, 'ImageUrls_ozon')

            # Применяем новый порядок столбцов к DataFrame
            df_final = df_final[cols]

            if not df_final.empty:
                # Подготавливаем данные
                headers = df_final.columns.tolist()
                
                # Конвертируем все значения в строки и заменяем NaN
                data_to_write = [headers]
                for _, row in df_final.iterrows():
                    row_data = []
                    for col in headers:
                        value = row[col] if col in row else ''
                        if pd.isna(value):
                            value = ''
                        row_data.append(str(value))
                    data_to_write.append(row_data)
                
                # Записываем
                ws.clear()
                time.sleep(1)
                ws.update('A1', data_to_write)
                
                write_log(f"Записано {len(df_final)} строк в лист content")
                write_log(f"=== СТАТИСТИКА ===")
                write_log(f"Артикулов в файле: {len(file_data_dict)}")
                write_log(f"Артикулов в content: {len(df_final)}")
                
            else:
                write_log("Нет данных для записи")
            
            
        except Exception as e:
            error_msg = f"Ошибка обработки файла: {str(e)}"
            write_log(error_msg)
            ws_clrows.update_cell(len(ws_clrows.get_all_values()), 10, error_msg)
        
    # except RefreshError as e:
    #     write_log(f"Ошибка аутентификации Google: {e}")
    # except Exception as e:
    #     write_log(f"Неизвестная ошибка: {e}")
    #     import traceback
    #     write_log(f"Трассировка: {traceback.format_exc()}")

               # таблица где есть цены на комплектующие и какие комплектующие идут в вариации
        # df_price_kompl = get_config.down_respons(url_batya,406620284,"price_kompl.csv")
        # write_log("df_price_kompl end")
        
        # Удаляем строки с пустыми значениями в столбце 'ОЗОН. Вариации.'
        # df_price_kompl = df_price_kompl.dropna(subset=['ОЗОН. Вариации.'])
        
        
        
        # df_final = get_config.create_ozon_table(df,df_price_kompl,values,ozon_mapping)
        # df_final = get_config.create_ozon_table(df,df_price_kompl,values,df_mapping)




        # 1) безопасно очистим inf/NaN и приведём к native
        # df_final = df_final.replace([np.inf, -np.inf], np.nan)
        df_final = df.fillna("")          # безопасно для NaN
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
        get_config.finish("generate_ozon_content")

        write_status("finished", "контент создан", "generate_ozon_content")

        write_log(f"[generate_ozon_content] Выполнено в {datetime.now()}")
    
    except Exception as e:
        tb = traceback.format_exc()
        write_log("ERROR: " + str(e))
        write_log(tb)
        write_status("error", str(e), "generate_ozon_content")
    
        # for name_table in list_par_kom:
        #     print(f"обработка комлл - {name_table}")
        # final_table = merge_oz_param(final_table, name_table)
        


if __name__ == "__main__":
    main()
