import pandas as pd
import glob
# Путь к директории с файлами Excel
directory_path = "xls/stat_sum"  # Замените на ваш путь к директории

# Получение всех файлов Excel в указанной директории
files = glob.glob(f"{directory_path}/*.xlsx")

# Чтение файлов и объединение их в один DataFrame
dfs = []
for file in files:
    df = pd.read_excel(file)
    dfs.append(df)

# Объединение всех DataFrame в один
combined_data = pd.concat(dfs, ignore_index=True)

# Фильтрация данных по столбцу "Параметр" равному "Сваи"
filtered_data = combined_data[combined_data['Параметр'] == 'Сваи']
# Печать первых строк filtered_data для проверки
print(filtered_data.head())

# Создание сводной таблицы с группировкой по столбцу "город" и вычислением рейтинга
pivot_table = filtered_data.groupby('Город').apply(lambda x: (x['Запросов контактов на Avito'].sum() * 10 + x['Просмотров на Avito'].sum())).reset_index(name='рейтинг')

# Печать первых строк сводной таблицы для проверки
print(pivot_table.head())
# Сохранение сводной таблицы в CSV файл
pivot_table.to_csv(f'{directory_path}\pivot_table.csv', index=False, encoding='utf-8')

# Отображение первых строк сводной таблицы для проверки
print(pivot_table.head())