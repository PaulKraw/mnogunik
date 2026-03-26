import re
import random

def replace_synonyms(text):
    # Функция для выбора случайного синонима из блока
    def process_block(inner_text):
        # Убираем внешние $sin{ и $sin}
        inner_text = inner_text[5:-5].strip()
        # Разделяем на синонимы и выбираем случайный
        synonyms = inner_text.split('||')
        synonyms = [syn.strip() for syn in synonyms if syn.strip()]  # Убираем лишние пробелы
        return random.choice(synonyms)
    
    def process_text(text):
        # Функция для обработки вложенных конструкций
        def replace_match(match):
            full_match = match.group(0)
            inner_text = full_match[5:-5]
            return process_block(full_match)

        # Регулярное выражение для нахождения конструкций
        pattern = r'\$sin\{[^{}]*\s*\$sin\}'
        while re.search(pattern, text, flags=re.DOTALL):
            text = re.sub(pattern, replace_match, text, flags=re.DOTALL)
        
        return text
    
    # Обрабатываем текст
    processed_text = process_text(text)
    
    # Обрабатываем оставшиеся конструкции, если таковые имеются
    final_pattern = r'\$sin\{(.*?)\s*\$sin\}'
    while re.search(final_pattern, processed_text, flags=re.DOTALL):
        processed_text = re.sub(final_pattern, lambda match: process_block(match.group(0)), processed_text, flags=re.DOTALL)
    
    return processed_text


def replace_synonyms1(text):
    i = 0
    num_sim = len(text)
    result = []

    while i < num_sim:
        # Ищем начало конструкции $sin{
        if text[i:i+5] == '$sin{':
            start = i
            depth = 1
            i += 5
            
            # Ищем соответствующее закрытие $sin}
            while i < num_sim and depth > 0:
                if text[i:i+5] == '$sin{':
                    depth += 1
                elif text[i:i+5] == '$sin}':
                    depth -= 1
                i += 1
            
            if depth == 0:
                # Найдено соответствующее закрытие
                end = i
                block = text[start:end]
                
                # Обрабатываем найденный блок
                processed_block = process_block(block)
                result.append(processed_block)
            else:
                # Ошибка: не найдено соответствующее закрытие
                result.append(text[start:i])
        else:
            # Добавляем текущий символ в результат
            result.append(text[i])
            i += 1
    
    return ''.join(result)
    

def process_block1(block):
    # Убираем внешние $sin{ и $sin}
    inner_text = block[5:-5].strip()
    
    # Разделяем на синонимы и выбираем случайный
    synonyms = inner_text.split('||')
    synonyms = [syn.strip() for syn in synonyms if syn.strip()]  # Убираем лишние пробелы
    return random.choice(synonyms)





# Пример использования
text = """$sin{ $sin{ 1 || 11 || 111 $sin}  || $sin{ 2 || 22 || 222 $sin}  || $sin{ 3 || 33 || 333 $sin}  $sin}
$sin{ A || B || C $sin}
$sin{ A || $sin{ B1 || B2 || B3 || $sin{ BB33 || BB333 || BB3333 $sin} $sin} || C $sin}"""

result = replace_synonyms(text)
print(result)
