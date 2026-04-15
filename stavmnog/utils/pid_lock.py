"""
stavmnog/utils/pid_lock.py — Защита от двойного запуска через PID-файл.
"""

import atexit
import os
import signal
import sys
from typing import Optional

from stavmnog.config import STATUS_DIR


def _pid_path(op_key: str, client_key: str) -> str:
    return os.path.join(STATUS_DIR, f"{op_key}_{client_key}.pid")


def _is_alive(pid: int) -> bool:
    """Проверка живости процесса через /proc — работает независимо от прав."""
    if pid <= 0:
        return False
    try:
        return os.path.exists(f"/proc/{pid}")
    except Exception:
        return False


def acquire_lock(op_key: str, client_key: str, logger=None) -> bool:
    os.makedirs(STATUS_DIR, exist_ok=True)
    path = _pid_path(op_key, client_key)

    if os.path.exists(path):
        try:
            old_pid = int(open(path).read().strip())
        except (ValueError, IOError):
            old_pid = 0

        if _is_alive(old_pid):
            if logger:
                logger.warning(f"{op_key} {client_key} уже запущен (pid={old_pid})")
            return False

        # Мёртвый PID — удаляем файл
        if logger:
            logger.info(f"Найден мёртвый pid={old_pid}, очищаю и продолжаю")
        try:
            os.remove(path)
        except Exception:
            pass

    # Записываем свой PID
    with open(path, "w") as f:
        f.write(str(os.getpid()))

    def _cleanup():
        try:
            if os.path.exists(path):
                # Убеждаемся что удаляем именно свой файл
                cur = open(path).read().strip()
                if cur == str(os.getpid()):
                    os.remove(path)
        except Exception:
            pass

    atexit.register(_cleanup)

    def _sig_handler(sig, frame):
        _cleanup()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _sig_handler)
    return True


def release_lock(op_key: str, client_key: str) -> None:
    path = _pid_path(op_key, client_key)
    try:
        if os.path.exists(path):
            cur = open(path).read().strip()
            if cur == str(os.getpid()):
                os.remove(path)
    except Exception:
        pass