"""
shared/config.py — Единая конфигурация монорепозитория mnogunik.

Все пути, ID таблиц, флаги среды читаются из .env.
Каждый модуль (generator, pricecraft, stavmnog) импортирует отсюда.
"""

import os
import socket
from typing import Optional

# ── Загрузка .env ──
try:
    from dotenv import load_dotenv

    # Ищем .env от корня монорепозитория (на 1 уровень выше shared/)
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
    load_dotenv(_env_path)
except ImportError:
    pass


# ═══════════════════════════════════════════
# СРЕДА
# ═══════════════════════════════════════════

HOSTNAME: str = socket.gethostname()
IS_LOCAL: bool = HOSTNAME in ("DESKTOP-USR21ET",)

# Алиас для обратной совместимости с generator
nout: bool = IS_LOCAL


# ═══════════════════════════════════════════
# ПУТИ
# ═══════════════════════════════════════════

# Корень монорепозитория (папка где лежит shared/)
MONOREPO_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Корень данных клиентов (proj/)
ROOT_DIR: str = os.environ.get("MNOGUNIK_ROOT_DIR", "")
if not ROOT_DIR:
    ROOT_DIR = "C:/proj" if IS_LOCAL else "/var/www/mnogunik.ru/proj"

# Выходные файлы
ROOT_DIR_OUT: str = os.environ.get(
    "MNOGUNIK_ROOT_DIR_OUT", os.path.join(ROOT_DIR, "outfile")
)

# URL для ссылок на картинки
ROOT_URL_OUT: str = os.environ.get(
    "MNOGUNIK_ROOT_URL_OUT",
    "http://localhost/outfile" if IS_LOCAL else "http://mnogunik.ru/outfile",
)

# SQLite база данных
DB_PATH: str = os.environ.get(
    "MNOGUNIK_DB_PATH",
    os.path.join(ROOT_DIR, "data", "avito.db") if not IS_LOCAL
    else os.path.join(ROOT_DIR, "avito.db"),
)


# ═══════════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════════

GOOGLE_CREDENTIALS_PATH: str = os.environ.get(
    "GOOGLE_CREDENTIALS_PATH", ""
)

SPREADSHEET_ID: str = os.environ.get(
    "SPREADSHEET_ID", "1fLrruYkw0JOOszb6q4bUpNYImflxFkdem57pQPc0qnQ"
)

SPREADSHEET_CLROWS_ID: str = os.environ.get(
    "SPREADSHEET_CLROWS_ID", "1tLVJCMAqYxzHw1SgwT8UcaM4eH6tccMhg8szjBnnkHk"
)

SPREADSHEET_URL: str = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"


# ═══════════════════════════════════════════
# OZON API
# ═══════════════════════════════════════════

OZON_CLIENT_ID: str = os.environ.get("OZON_CLIENT_ID", "")
OZON_API_KEY: str = os.environ.get("OZON_API_KEY", "")


# ═══════════════════════════════════════════
# WEB ПАНЕЛИ
# ═══════════════════════════════════════════

GENERATOR_WEB_KEY: str = os.environ.get("GENERATOR_WEB_KEY", "")
PRICECRAFT_WEB_PASSWORD: str = os.environ.get("PRICECRAFT_WEB_PASSWORD", "")
MNOGUNIK_RUN_URL: str = os.environ.get(
    "MNOGUNIK_RUN_URL", "https://mnogunik.ru/generator/run.php"
)
