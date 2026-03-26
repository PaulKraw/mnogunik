import os
import pandas as pd
import sys

# Добавляем путь к textfun.py (он на уровень выше)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import textfun  # Импортируем сам модуль, будем использовать textfun.replace_synonyms

# Пути к папкам
base_dir = os.path.dirname(__file__)
template_path = os.path.join(base_dir, 'template')
return_path = os.path.join(base_dir, 'return')

# Создаём папку return, если её нет
os.makedirs(return_path, exist_ok=True)

# Обработка всех CSV файлов
for filename in os.listdir(template_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(template_path, filename)
        try:
            df = pd.read_csv(file_path, dtype=str)
        except Exception as e:
            print(f'❌ Ошибка при чтении {filename}: {e}')
            continue

        # Обработка колонки Description
        if 'Description' in df.columns:
            df['Description'] = df['Description'].apply(
                lambda text: textfun.replace_synonyms(text) if pd.notnull(text) else text
            )

            # Сохраняем результат
            save_path = os.path.join(return_path, filename)
            df.to_csv(save_path, index=False)
            print(f'✅ Обработан: {filename}')
        else:
            print(f'⚠️ В файле {filename} нет колонки "Description"')
