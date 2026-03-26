import pandas as pd

def filter_columns(input_csv, output_csv, columns_to_keep):
    # Чтение исходного CSV файла
    df = pd.read_csv(input_csv)
    
    # Проверка, что все указанные столбцы существуют в исходном файле
    missing_columns = [col for col in columns_to_keep if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Следующие столбцы отсутствуют в CSV файле: {', '.join(missing_columns)}")
    
    # Оставляем только указанные столбцы
    df_filtered = df[columns_to_keep]
    
    # Запись результата в новый CSV файл
    df_filtered.to_csv(output_csv, index=False)
    print(f"Файл успешно сохранен как {output_csv}")

# Список параметров
parameters = {
    'input_csv': 'input.csv',  # путь к исходному CSV файлу
    'output_csv': 'output.csv',  # путь к новому CSV файлу
    'columns_to_keep': ['column1', 'column2', 'column3']  # столбцы, которые нужно оставить
}

# Вывод всех параметров
print(f"Введенные параметры: {parameters}")

# Основной блок
if __name__ == "__main__":
    filter_columns(parameters['input_csv'], parameters['output_csv'], parameters['columns_to_keep'])
