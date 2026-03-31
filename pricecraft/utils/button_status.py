"""
utils/button_status.py — Управление статусами кнопок на фронтенде.

Заменяет get_config.finish() — теперь один источник.
"""

import json
import os
from datetime import datetime

from pricecraft.config.settings import BUTTONS_STATUS_FILE


def finish(action: str) -> None:
    """
    Помечает действие как завершённое в buttons_status.json.

    Args:
        action: Имя модуля/действия (create_configurations, generate_ozon_file и т.д.).
    """
    with open(BUTTONS_STATUS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if action in data:
        data[action]["status"] = "done"
        data[action]["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(BUTTONS_STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def set_generating(action: str) -> None:
    """Помечает действие как «в процессе»."""
    with open(BUTTONS_STATUS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if action in data:
        data[action]["status"] = "generating"
        data[action]["generated_at"] = None

    with open(BUTTONS_STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
