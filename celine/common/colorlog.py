# celine/common/colorlog.py
from __future__ import annotations
import logging

# ANSI colors
RESET = "\033[0m"
BOLD = "\033[1m"

COLORS = {
    logging.DEBUG: "\033[36m",  # Cyan
    logging.INFO: "\033[32m",  # Green
    logging.WARNING: "\033[33m",  # Yellow
    logging.ERROR: "\033[31m",  # Red
    logging.CRITICAL: "\033[35m",  # Magenta
}


class ColorFormatter(logging.Formatter):
    """
    A simple colored log formatter for CLI usage.
    Activated only when running inside the CLI.
    """

    def format(self, record: logging.LogRecord) -> str:
        level_color = COLORS.get(record.levelno, "")
        message = super().format(record)
        return f"{level_color}{message}{RESET}"
