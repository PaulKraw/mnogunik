import requests
import csv
import datetime
import time
import pandas as pd

start_time = time.time()

ROOT_DIR = 'D:/proj/'
name = "svai"


ads = f"{ROOT_DIR}/{name}/ads.xlsx"
id_avitoid_file = f"{ROOT_DIR}/{name}/var/id_avitoid.csv"


def convert_excel_to_csv(input_file, output_file):
    try:
        # Чтение данных из Excel
        df = pd.read_excel(input_file)
        
        # Отбираем только нужные столбцы
        df_filtered = df[['AvitoId', 'Id']]
        
        # Сохранение в CSV
        df_filtered.to_csv(output_file, index=False)
        
        print(f"Данные успешно сохранены в файл: {output_file}")
    except Exception as e:
        print(f"Произошла ошибка при конвертации данных: {e}")

convert_excel_to_csv(ads, id_avitoid_file)

# Конец выполнения программы
end_time = time.time()

# Время выполнения программы
execution_time = end_time - start_time
print(f"Время выполнения программы: {execution_time:.2f} секунд")
# Текущее время
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"Текущее время: {current_time}")

if __name__ == "__main__":
    print("Конец.")
else:
    print("my_module.py has been imported.")