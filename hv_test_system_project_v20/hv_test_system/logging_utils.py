from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

_FMT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

def setup_logger(name: str = "hv_test_system", log_dir: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if getattr(logger, "_hv_configured", False):
        return logger
    logger.setLevel(level)
    logger.propagate = False
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(_FMT))
    logger.addHandler(ch)
    base = Path(log_dir) if log_dir else Path("logs")
    base.mkdir(parents=True, exist_ok=True)
    fh = RotatingFileHandler(base / "app.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(_FMT))
    logger.addHandler(fh)
    logger._hv_configured = True
    return logger
