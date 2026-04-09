"""
stavmnog/config.py — Настройки модуля статистики и ставок.

Общие пути и секреты — в shared/config.py.
Здесь: Avito API метрики, маппинг колонок Sheets, rate limits.
"""

import json
import os

from shared.config import MONOREPO_ROOT, DB_PATH

# ═══════════════════════════════════════════
# ПУТИ (stavmnog-специфичные)
# ═══════════════════════════════════════════
# define('MONOREPO_ROOT', '/var/www/mnogunik.ru/mng');

STAVMNOG_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR   = os.path.join(MONOREPO_ROOT, "db//config")
STATUS_DIR   = os.path.join(STAVMNOG_DIR, "web/status")
LOG_DIR      = os.path.join(STAVMNOG_DIR, "web/logs")

CLIENTS_JSON = os.path.join(MONOREPO_ROOT, "/db//config/clients.json")





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
# GOOGLE SHEETS — маппинг колонок листа «Ставки»
# Буква → индекс (1-based)
# ═══════════════════════════════════════════

COL = {
    "Id":           1,   # A
    "AvitoId":      2,   # B
    "Ставка":       3,   # C
    "Лимит":        4,   # D
    "name":         5,   # E
    "name_csv":     6,   # F
    "лим_клик":     7,   # G
    "корект":       8,   # H  ← prev_bid
    "кол":          9,   # I
    "пр_став":      10,  # J
    "мин":          11,  # K  ← min_bid
    "макс":         12,  # L  ← max_bid
    "цен":          13,  # M
    "pct_7d":       14,  # N
    "cpl_7d":       15,  # O
    "clicks_7d":    16,  # P
    "leads_7d":     17,  # Q
    "pct_prev":     18,  # R
    "cpl_prev":     19,  # S
    "clicks_prev":  20,  # T
    "leads_prev":   21,  # U
    "bid_code":     22,  # V
    "limit_code":   23,  # W
    "Статус":       24,  # X
    "Сообщение":    25,  # Y
    "Применил":     26,  # Z
    "дата_прим":    27,  # AA
}
