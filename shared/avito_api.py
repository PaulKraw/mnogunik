"""
shared/avito_api.py — Работа с API Авито и Ozon для всех модулей.

Заменяет:
- stavmnog: get_access_token(), safe_post(), safe_get() (в каждом скрипте)
- pricecraft: OZON_HEADERS, HEADERS_combat (хардкод в 4 файлах)

Использование:
    from shared.avito_api import get_avito_token, avito_post, get_ozon_headers
"""

import json
import os
import time
from typing import Any, Dict, Optional

import requests

from shared.config import ROOT_DIR, OZON_CLIENT_ID, OZON_API_KEY
from shared.logger import write_log


# ═══════════════════════════════════════════
# AVITO API
# ═══════════════════════════════════════════

AVITO_TOKEN_URL = "https://api.avito.ru/token"


def get_avito_credentials(client_name: str) -> Dict[str, str]:
    """
    Получает API-ключи клиента Авито.

    Приоритет:
    1. Переменные окружения: AVITO_CLIENT_ID_{name}, AVITO_CLIENT_SECRET_{name}
    2. JSON-файл: ROOT_DIR/{client}/var/api.json → {account_key}

    Args:
        client_name: Имя клиента в формате "svai_alx" (client_account).

    Returns:
        Словарь с CLIENT_ID, CLIENT_SECRET, USER_ID.
    """
    # 1. Из .env
    env_prefix = client_name.upper().replace("-", "_")
    client_id = os.environ.get(f"AVITO_CLIENT_ID_{env_prefix}", "")
    client_secret = os.environ.get(f"AVITO_CLIENT_SECRET_{env_prefix}", "")
    user_id = os.environ.get(f"AVITO_USER_ID_{env_prefix}", "")

    if client_id and client_secret:
        return {
            "CLIENT_ID": client_id,
            "CLIENT_SECRET": client_secret,
            "USER_ID": user_id,
        }

    # 2. Из JSON-файла (обратная совместимость)
    parts = client_name.split("_", 1)
    if len(parts) == 2:
        client_dir, account_key = parts
    else:
        client_dir, account_key = client_name, client_name

    json_path = os.path.join(ROOT_DIR, client_dir, "var", "api.json")
    if os.path.isfile(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if account_key in data:
            return data[account_key]

    raise ValueError(f"API-ключи не найдены для клиента: {client_name}")


def get_avito_token(client_id: str, client_secret: str) -> str:
    """
    Получает access_token Авито API.

    Args:
        client_id: Client-Id приложения.
        client_secret: Client-Secret.

    Returns:
        access_token.
    """
    resp = requests.post(
        AVITO_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise ValueError(f"Не удалось получить токен: {resp.json()}")
    return token


def avito_get(
    url: str,
    headers: Dict[str, str],
    retries: int = 3,
    timeout: int = 10,
) -> Optional[requests.Response]:
    """
    GET-запрос к Avito API с retry.

    Args:
        url: URL запроса.
        headers: Заголовки (включая Authorization).
        retries: Количество попыток.
        timeout: Таймаут в секундах.

    Returns:
        Response или None при неудаче.
    """
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.ConnectionError as e:
            write_log(f"[{attempt + 1}/{retries}] Ошибка соединения: {e}")
            time.sleep(2)
        except requests.exceptions.RequestException as e:
            write_log(f"Ошибка GET: {e}")
            break
    return None


def avito_post(
    url: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    retries: int = 3,
    timeout: int = 10,
    delay: float = 2,
) -> Optional[requests.Response]:
    """
    POST-запрос к Avito API с retry и exponential backoff.

    Args:
        url: URL запроса.
        headers: Заголовки.
        payload: Тело запроса.
        retries: Количество попыток.
        timeout: Таймаут.
        delay: Начальная задержка между попытками.

    Returns:
        Response или None.
    """
    current_delay = delay
    for attempt in range(retries):
        try:
            resp = requests.post(
                url, headers=headers, json=payload, timeout=timeout
            )
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                write_log(
                    f"[{attempt + 1}/{retries}] Сервер {resp.status_code}, "
                    f"пауза {current_delay}с"
                )
                time.sleep(current_delay)
                current_delay *= 2
            elif resp.status_code == 403:
                write_log(f"[{attempt + 1}/{retries}] Доступ запрещён (403)")
                time.sleep(current_delay)
                current_delay *= 2
            else:
                write_log(f"Ошибка {resp.status_code}: {resp.text[:200]}")
                return resp
        except requests.exceptions.Timeout:
            write_log(f"[{attempt + 1}/{retries}] Таймаут, пауза {current_delay}с")
            time.sleep(current_delay)
            current_delay *= 2
        except requests.exceptions.ConnectionError as e:
            write_log(f"[{attempt + 1}/{retries}] Соединение: {e}")
            time.sleep(current_delay)
            current_delay *= 2
        except requests.exceptions.RequestException as e:
            write_log(f"Неожиданная ошибка: {e}")
            break

    write_log("❌ Все попытки исчерпаны")
    return None


# ═══════════════════════════════════════════
# OZON API
# ═══════════════════════════════════════════

def get_ozon_headers() -> Dict[str, str]:
    """
    Возвращает заголовки для Ozon API из .env.

    Returns:
        Словарь с Client-Id, Api-Key, Content-Type.
    """
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json",
    }


OZON_PRICE_API_URL = "https://api-seller.ozon.ru/v1/product/import/prices"
