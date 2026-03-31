"""
config/settings.py — Пути, константы, параметры проекта.

Секреты вынесены в config/secrets.py → .env.
"""

import os
import sys

# ── Пути ──
PROJECT_ROOT = os.environ.get(
    "PRICECRAFT_ROOT",
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

RUNNERS_DIR = os.path.join(PROJECT_ROOT, "runners")
LOG_FILE = os.path.join(RUNNERS_DIR, "log.txt")
STATUS_FILE = os.path.join(RUNNERS_DIR, "status.json")
BUTTONS_STATUS_FILE = os.path.join(RUNNERS_DIR, "buttons_status.json")

# ── Google Sheets (ID из secrets) ──
from shared.config import SPREADSHEET_ID, SPREADSHEET_CLROWS_ID

url_batya = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"

# ── API ──
BATCH_SIZE = 950
DELAY_BETWEEN_BATCHES = 2


def get_values(wsprices) -> dict:
    """Читает ключевые коэффициенты из листа Price_new."""
    return {
        "sborka": wsprices.acell("K8").value,
        "proch": wsprices.acell("K9").value,
        "avito": wsprices.acell("C2").value,
        "fixavito": wsprices.acell("D2").value,
        "ozon": wsprices.acell("E2").value,
        "wb": wsprices.acell("F2").value,
    }
