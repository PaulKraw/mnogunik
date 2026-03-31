"""
shared/db.py — Единое подключение к SQLite для всех модулей.

Все модули (generator, pricecraft, stavmnog, panel) работают с одной БД.

Использование:
    from shared.db import get_connection, execute, init_db
"""

import os
import sqlite3
from typing import Any, List, Optional, Tuple

from shared.config import DB_PATH


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """
    Возвращает подключение к SQLite.

    Args:
        db_path: Путь к БД. Если None — из .env (DB_PATH).

    Returns:
        sqlite3.Connection.
    """
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row  # доступ по имени колонки
    conn.execute("PRAGMA journal_mode=WAL")  # лучше для конкурентного доступа
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def execute(
    sql: str,
    params: Optional[Tuple] = None,
    db_path: Optional[str] = None,
) -> sqlite3.Cursor:
    """
    Выполняет SQL-запрос и коммитит.

    Args:
        sql: SQL-строка.
        params: Параметры для подстановки.
        db_path: Путь к БД.

    Returns:
        sqlite3.Cursor с результатом.
    """
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(sql, params or ())
        conn.commit()
        return cursor
    finally:
        conn.close()


def executemany(
    sql: str,
    params_list: List[Tuple],
    db_path: Optional[str] = None,
) -> int:
    """
    Выполняет SQL для множества строк (INSERT/UPDATE батчами).

    Args:
        sql: SQL-строка с плейсхолдерами.
        params_list: Список кортежей параметров.
        db_path: Путь к БД.

    Returns:
        Количество затронутых строк.
    """
    conn = get_connection(db_path)
    try:
        cursor = conn.executemany(sql, params_list)
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()


def fetch_all(
    sql: str,
    params: Optional[Tuple] = None,
    db_path: Optional[str] = None,
) -> List[sqlite3.Row]:
    """
    Выполняет SELECT и возвращает все строки.

    Args:
        sql: SELECT-запрос.
        params: Параметры.
        db_path: Путь к БД.

    Returns:
        Список sqlite3.Row (доступ по имени колонки).
    """
    conn = get_connection(db_path)
    try:
        return conn.execute(sql, params or ()).fetchall()
    finally:
        conn.close()


def fetch_one(
    sql: str,
    params: Optional[Tuple] = None,
    db_path: Optional[str] = None,
) -> Optional[sqlite3.Row]:
    """Выполняет SELECT и возвращает одну строку."""
    conn = get_connection(db_path)
    try:
        return conn.execute(sql, params or ()).fetchone()
    finally:
        conn.close()


def init_db(schema_path: Optional[str] = None, db_path: Optional[str] = None) -> None:
    """
    Создаёт все таблицы из schema.sql.

    Args:
        schema_path: Путь к SQL-файлу. По умолчанию: db/schema.sql в корне монорепо.
        db_path: Путь к БД.
    """
    from shared.config import MONOREPO_ROOT

    if schema_path is None:
        schema_path = os.path.join(MONOREPO_ROOT, "db", "schema.sql")

    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Схема БД не найдена: {schema_path}")

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    conn = get_connection(db_path)
    try:
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()
