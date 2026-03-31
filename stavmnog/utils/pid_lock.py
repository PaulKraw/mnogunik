"""
stavmnog/utils/pid_lock.py — Защита от двойного запуска через PID-файл.

Заменяет 3 копии PID-lock логики (download, build_stats, export_stats).

Использование:
    from stavmnog.utils.pid_lock import acquire_lock, release_lock

    if not acquire_lock("download", "evg", logger):
        return  # уже запущен
    try:
        do_work()
    finally:
        release_lock("download", "evg")
"""

import atexit
import os
import signal
from typing import Optional

from stavmnog.config import STATUS_DIR


def _pid_path(op_key: str, client_key: str) -> str:
    return os.path.join(STATUS_DIR, f"{op_key}_{client_key}.pid")


def acquire_lock(op_key: str, client_key: str, logger=None) -> bool:
    """
    Пытается захватить PID-lock.

    Args:
        op_key: Имя операции (download, build_stats, export_stats, bids).
        client_key: Ключ клиента.
        logger: Логгер для сообщений.

    Returns:
        True если lock захвачен, False если процесс уже запущен.
    """
    os.makedirs(STATUS_DIR, exist_ok=True)
    path = _pid_path(op_key, client_key)

    # Проверяем живой ли старый процесс
    if os.path.exists(path):
        try:
            old_pid = int(open(path).read().strip())
            try:
                os.kill(old_pid, 0)  # проверка без убийства
                if logger:
                    logger.warning(
                        f"{op_key} {client_key} уже запущен (pid={old_pid})"
                    )
                return False
            except (OSError, ProcessLookupError):
                pass  # процесс мёртв — можно продолжать
        except (ValueError, IOError):
            pass

    # Записываем свой PID
    with open(path, "w") as f:
        f.write(str(os.getpid()))

    # Гарантируем очистку при любом завершении
    def _cleanup():
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

    atexit.register(_cleanup)

    def _sig_handler(sig, frame):
        _cleanup()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _sig_handler)

    return True


def release_lock(op_key: str, client_key: str) -> None:
    """Удаляет PID-файл."""
    path = _pid_path(op_key, client_key)
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass
