from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

DEFAULT_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(
    name: str,
    log_dir: str | None = None,
    level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """Return a logger that writes to stdout and a rotating file.

    File path: {LOG_PATH or log_dir}/scripts/{name}.log
    Idempotent — repeated calls do not stack handlers.
    """
    logger = logging.getLogger(name)
    if getattr(logger, "_r2_configured", False):
        return logger

    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(DEFAULT_FORMAT)

    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    base = Path(log_dir or os.environ.get("LOG_PATH", "logs")) / "scripts"
    base.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        base / f"{name}.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    setattr(logger, "_r2_configured", True)
    return logger
