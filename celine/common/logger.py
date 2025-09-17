import logging
import os
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

    # Suppress only the single warning from urllib3
    warnings.filterwarnings("ignore", category=InsecureRequestWarning)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a configured logger instance with custom level for celine.* modules.

    Args:
        name: The name of the logger. If None, uses the module name.

    Returns:
        Configured logger instance with appropriate level for celine.* modules.
    """
    # Use the module name if no name is provided
    if name is None:
        import sys

        name = sys._getframe(1).f_globals.get("__name__", "root")

    logger = logging.getLogger(name)

    # Only configure if this is the first time for this logger
    if not len(logger.handlers):
        # Determine the log level based on whether it's a celine module
        if str(name).startswith("celine."):
            # Get log level specifically for celine modules
            log_level = os.getenv("CELINE_LOG_LEVEL", "DEBUG").upper()
        else:
            # Use default log level for other modules
            log_level = os.getenv("LOG_LEVEL", "INFO").upper()

        # Convert string level to logging level constant
        level = getattr(logging, log_level, logging.INFO)

        # Set logger level
        logger.setLevel(level)

        # Create console handler
        # handler = logging.StreamHandler()

        # # Use a consistent formatter for all modules
        # formatter = logging.Formatter(
        #     "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        #     datefmt="%Y-%m-%d %H:%M:%S",
        # )

        # handler.setFormatter(formatter)
        # logger.addHandler(handler)

    return logger


# Configure logging when the module is imported
configure_logging()
