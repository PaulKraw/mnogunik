"""
shared/logger.py — Единое логирование для всех модулей mnogunik.

Заменяет:
- generator: utils/logging.py (print_log)
- pricecraft: utils/module_logger.py (write_log, write_status)
- stavmnog: write_log() в каждом скрипте

Использование:
    from shared.logger import write_log, write_status, get_logger
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from shared.config import IS_LOCAL

_logger: Optional[logging.Logger] = None


def get_logger(
    name: str = "mnogunik",
    log_file: str = "log.txt",
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Возвращает (или создаёт) единый логгер.

    Args:
        name: Имя логгера.
        log_file: Путь к файлу лога.
        level: Уровень логирования.

    Returns:
        Настроенный logging.Logger.
    """
    global _logger
    if _logger is not None:
        return _logger

    logger = logging.getLogger(name)
    logger.setLevel(level)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Файл
    os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
    fh = logging.FileHandler(log_file, encoding="utf-8", mode="a")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Консоль
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    _logger = logger
    return logger


def write_log(msg: str, log_file: Optional[str] = None) -> None:
    """
    Записывает сообщение в лог-файл и stdout.

    Drop-in замена для write_log() из всех трёх модулей.

    Args:
        msg: Текст сообщения.
        log_file: Путь к лог-файлу (если None — используется log.txt).
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}\n"
    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(line)
    print(msg)  # всегда в stdout — оттуда nohup пишет в лог процесса

def write_status(
    status: str,
    message: str = "",
    module: str = "unknown",
    status_file: Optional[str] = None,
) -> None:
    """
    Обновляет JSON-файл статуса модуля.

    Args:
        status: running / idle / error / finished.
        message: Описание текущего шага.
        module: Имя модуля.
        status_file: Путь к status.json.
    """
    data = {
        "module": module,
        "status": status,
        "message": message,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    if status_file:
        os.makedirs(os.path.dirname(status_file) or ".", exist_ok=True)
        with open(status_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


# Обратная совместимость с generator
print_log = write_log


def reset_log(log_file: str = "log.txt") -> None:
    """Очищает файл лога."""
    with open(log_file, "w") as f:
        f.truncate(0)
