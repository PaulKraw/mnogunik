import os
import pandas as pd

def csv_files_to_excel(folder_path, output_folder):
    # Получаем все файлы .csv из папки
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            csv_file = os.path.join(folder_path, file_name)
            excel_file = os.path.join(output_folder, f"{os.path.splitext(file_name)[0]}.xlsx")
            
            # Чтение CSV-файла
            df = pd.read_csv(csv_file)
            
            # Сохранение в Excel
            df.to_excel(excel_file, index=False, engine='openpyxl')
            print(f"Файл {csv_file} успешно конвертирован в {excel_file}")

# Пример использования
csv_files_to_excel('csvxls/for_csv', 'csvxls/output_excel')