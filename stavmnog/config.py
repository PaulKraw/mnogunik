"""
stavmnog/config.py — Настройки модуля статистики и ставок.

Общие пути и секреты — в shared/config.py.
Здесь: Avito API метрики, маппинг колонок Sheets, rate limits.
"""

import json
import os

from shared.config import MONOREPO_ROOT

# ═══════════════════════════════════════════
# ПУТИ (stavmnog-специфичные)
# ═══════════════════════════════════════════
# define('MONOREPO_ROOT', '/var/www/mnogunik.ru/mng');

STAVMNOG_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR   = os.path.join(MONOREPO_ROOT, "db", "config")
STATUS_DIR   = os.path.join(STAVMNOG_DIR, "web", "status")
LOG_DIR      = os.path.join(STAVMNOG_DIR, "web", "logs")
CLIENTS_JSON = os.path.join(CONFIG_DIR, "clients.json")



def load_clients() -> dict:
    """Загружает конфиг клиентов из clients.json."""
    if not os.path.exists(CLIENTS_JSON):
        return {}
    with open(CLIENTS_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def get_client_config(client_key: str) -> dict:
    """Возвращает конфиг одного клиента или raise ValueError."""
    clients = load_clients()
    if client_key not in clients:
        raise ValueError(f"Клиент '{client_key}' не найден в clients.json")
    return clients[client_key]


# ═══════════════════════════════════════════
# AVITO API — статистика
# ═══════════════════════════════════════════

AVITO_STATS_URL = "https://api.avito.ru/stats/v2/accounts/{user_id}/items"
RATE_LIMIT_SEC  = 65

METRICS = [
    "impressions", "views", "contacts", "favorites",
    "presenceSpending", "promoSpending", "allSpending",
    "averageViewCost", "averageContactCost",
]

MONEY_SLUGS = {
    "presenceSpending", "promoSpending", "allSpending",
    "averageViewCost", "averageContactCost",
}

SLUG_TO_COL = {
    "impressions":        "impressions",
    "views":              "views",
    "contacts":           "contacts",
    "favorites":          "favorites",
    "presenceSpending":   "presence_spend",
    "promoSpending":      "promo_spend",
    "allSpending":        "all_spend",
    "averageViewCost":    "avg_view_cost",
    "averageContactCost": "avg_contact_cost",
}


# ═══════════════════════════════════════════
# КАНОНИЧЕСКИЕ ИМЕНА КОЛОНОК ЛИСТА «Ставки»
# Порядок = порядок колонок A, B, C, ... в листе
# ═══════════════════════════════════════════

SHEET_COLUMNS = [
    "Id",               # A
    "AvitoId",          # B
    "Ставка",           # C
    "Лимит",            # D
    "name",             # E
    "name_csv",         # F
    "лим клик",         # G
    "корект",           # H
    "кол",              # I
    "пр став",          # J
    "мин",              # K
    "макс",             # L
    "цен",              # M
    "%",                # N  — CTR 7д
    "ц лид",            # O  — CPL 7д
    "клики",            # P
    "лид",              # Q
    "расход 7д",        # R  — НОВАЯ
    "% пред",           # S
    "ц лид пред",       # T
    "клики пред",       # U
    "лид пред",         # V
    "расход пред",      # W  — НОВАЯ
    "ставка из кода",   # X
    "лимит из кода",    # Y
    "Статус",           # Z
    "Сообщение",        # AA
    "!Применил",        # AB
    "дата применения",  # AC
]