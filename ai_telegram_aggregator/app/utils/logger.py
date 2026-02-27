"""Centralized logger setup."""
from __future__ import annotations

import logging
import sys


def setup_logger(level: str = "INFO") -> None:
    """Configure root logger for structured console output."""
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
