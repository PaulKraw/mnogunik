"""
config/sheets.py — Подключение к Google Sheets через сервисный аккаунт.
"""

import json
import os

import gspread

from shared.config import SPREADSHEET_ID

from shared.config import GOOGLE_CREDENTIALS_PATH
CREDENTIALS_FILE = GOOGLE_CREDENTIALS_PATH


def get_client() -> gspread.Client:
    """Возвращает авторизованный gspread-клиент."""
    with open(CREDENTIALS_FILE) as f:
        creds = json.load(f)
    return gspread.service_account_from_dict(creds)


def get_sheet(client: gspread.Client, sheet_name: str):
    """Открывает лист по имени (создаёт если не существует)."""
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        return sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=sheet_name, rows=10, cols=5)
