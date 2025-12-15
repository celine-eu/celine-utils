import logging
import os
import sys
from celine.common.colorlog import ColorFormatter
from typing import Optional
import warnings
from urllib3.exceptions import InsecureRequestWarning


def configure_logging():
    """Configure the root logger and suppress noisy libraries."""
    # Set root logger level
    root_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, root_level, logging.INFO))

    # Suppress noisy libraries
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("openlineage").setLevel(logging.INFO)

    # Suppress only the single warning from urllib3
    warnings.filterwarnings("ignore", category=InsecureRequestWarning)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    if name is None:
        name = sys._getframe(1).f_globals.get("__name__", "root")

    name = str(name) if name else "celine.default"
    logger = logging.getLogger(name)

    # If already configured → return it
    if logger.handlers:
        return logger

    # Determine log level
    if str(name).startswith("celine."):
        log_level = os.getenv("CELINE_LOG_LEVEL", "DEBUG").upper()
    else:
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    logger.setLevel(getattr(logging, log_level, logging.INFO))

    stdout_pipe = sys.stdout
    handler = logging.StreamHandler(stdout_pipe)

    # Enable color only for CLI
    use_color = stdout_pipe.isatty()

    if use_color:
        fmt = "%(levelname)s | %(message)s"
        handler.setFormatter(ColorFormatter(fmt))
        logger.propagate = False  # IMPORTANT: avoid double logging
    else:
        fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        handler.setFormatter(logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S"))

    logger.addHandler(handler)
    return logger


# Configure logging when the module is imported
configure_logging()
