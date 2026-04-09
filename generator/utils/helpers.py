"""
utils/helpers.py — Мелкие утилиты, не привязанные к конкретному модулю.

Собраны из textfun.py для устранения дублирования.
"""

import random
import re
import string
import time
from typing import List, Optional


def generate_random_code(length: int) -> str:
    """Генерирует случайный числовой код заданной длины."""
    return "".join(random.choice(string.digits) for _ in range(length))


def generate_random_char_code(length: int) -> str:
    """Генерирует случайный код из цифр + латинских букв."""
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def generate_random_hex_color() -> str:
    """Генерирует случайный тёмный HEX-цвет (#RRGGBB)."""
    r = random.randint(0, 50)
    g = random.randint(0, 50)
    b = random.randint(0, 50)
    return f"#{r:02X}{g:02X}{b:02X}"


def natural_sort_key(s: str) -> List:
    """Ключ для натуральной сортировки строк (1, 2, 10 вместо 1, 10, 2)."""
    return [
        int(text) if text.isdigit() else text.lower()
        for text in re.split(r"(\d+)", s)
    ]


def smart_format(val: float) -> str:
    """Округляет число: 5.0 → '5', 5.25 → '5.25'."""
    val = round(val, 2)
    if val == int(val):
        return str(int(val))
    return f"{val:.2f}"


def format_execution_time(seconds: float) -> str:
    """Форматирует время выполнения: '3 минуты (185.20 секунд)'."""
    minutes = int(seconds // 60)
    secs = seconds % 60

    def _word(m: int) -> str:
        if 11 <= m % 100 <= 19:
            return "минут"
        r = m % 10
        if r == 1:
            return "минута"
        if 2 <= r <= 4:
            return "минуты"
        return "минут"

    return f"Время выполнения: {minutes} {_word(minutes)} ({secs:.2f} секунд)"


def strip_html_tags(text: str) -> str:
    """Удаляет HTML-теги из текста."""
    return re.sub(r"<.*?>", "", text)


def path_to_html_link(local_path: str, text: str = None, base_url: str = "https://mnogunik.ru") -> str:
    """
    Преобразует локальный путь в HTML-ссылку <a> с target="_blank".

    Пример:
        >>> path = "/var/www/mnogunik.ru/proj/svmy/svmy_usl_2026-04-08_102.xml"
        >>> path_to_html_link(path)
        '<a href="https://mnogunik.ru/proj/svmy/svmy_usl_2026-04-08_102.xml" target="_blank">svmy_usl_2026-04-08_102.xml</a>'
    """
    # Убираем префикс /var/www/mnogunik.ru
    relative_path = local_path.replace("/var/www/mnogunik.ru", "")
    # Формируем полный URL
    full_url = base_url + relative_path
    # Берём имя файла для текста ссылки
    link_text = text if text else local_path.split("/")[-1]

    # Возвращаем HTML
    return f'<a href="{full_url}" target="_blank">{link_text}</a>'


