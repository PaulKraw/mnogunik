# config/settings.py
# Обязательные параметры для update_ozon_prices.py
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))  # это src/pricecraft/modules
PROJECT_ROOT = os.path.dirname(PROJECT_ROOT)               # это src/pricecraft
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


OZON_API_URL = "https://api-seller.ozon.ru/v1/product/import/prices"
BATCH_SIZE = 950
DELAY_BETWEEN_BATCHES = 2

# Пути к файлам (подкорректируй по проекту)
# PROJECT_ROOT = ".."  # не обязательно
FIN_TABLE_PATH = "csv/fin_table.csv"           # путь относительно корня проекта (pricecraft/)
PRODUCTS_FILE_PATH = "csv/products_combat.csv" # как у тебя в run.py

SPREADSHEET_ID = "1fLrruYkw0JOOszb6q4bUpNYImflxFkdem57pQPc0qnQ"
SPREADSHEET_CLROWS_ID = "1tLVJCMAqYxzHw1SgwT8UcaM4eH6tccMhg8szjBnnkHk"

url_batya = f'https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}'

def get_values(wsprices):
    values = {
        "sborka": wsprices.acell("K8").value,
        "proch": wsprices.acell("K9").value,
        "avito": wsprices.acell("C2").value,
        "fixavito": wsprices.acell("D2").value,
        "ozon": wsprices.acell("E2").value,
        "wb": wsprices.acell("F2").value
    }
    return values