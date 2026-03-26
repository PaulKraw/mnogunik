#!/usr/bin/env python3
# modules/stop_check.py - Простая проверка стоп-флага
import os
import sys

# Добавляем путь к PROJECT_ROOT
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from config import settings

PROJECT_ROOT = settings.PROJECT_ROOT
STOP_FLAG = os.path.join(PROJECT_ROOT, 'stop.flag')

def should_stop():
    """Проверить, нужно ли остановиться"""
    return os.path.exists(STOP_FLAG)

def check_stop():
    """Проверить и выйти если есть стоп-флаг"""
    if should_stop():
        print("🛑 Обнаружен стоп-флаг, останавливаюсь...")
        sys.exit(0)

def set_stop_flag():
    """Создать стоп-флаг"""
    with open(STOP_FLAG, 'w') as f:
        f.write('stop')
    print("🛑 Стоп-флаг установлен")

def clear_stop_flag():
    """Удалить стоп-флаг"""
    if os.path.exists(STOP_FLAG):
        os.remove(STOP_FLAG)
        print("🟢 Стоп-флаг удален")
    else:
        print("ℹ️ Стоп-флаг не найден")

def is_stopped():
    """Для фронта: проверка состояния"""
    return os.path.exists(STOP_FLAG)



# В любой длительной функции (например, в actualize_ozon.py):

# python
# # Добавить в начало файла
# from stop_check import check_stop

# # В цикле обработки (например, при архивации пакетов):
# for i in range(0, len(articles), BATCH_SIZE):
#     check_stop()  # <-- Проверяем здесь!
#     batch = articles[i:i+BATCH_SIZE]
#     # ... остальной код


# В функциях с множественными шагами:

# python
# def main():
#     try:
#         # Шаг 1
#         check_stop()
#         do_step_1()
        
#         # Шаг 2  
#         check_stop()
#         do_step_2()
        
#         # Шаг 3
#         check_stop()
#         do_step_3()