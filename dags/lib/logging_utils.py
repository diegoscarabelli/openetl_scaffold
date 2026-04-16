"""
Simple logger.

Respects LOG_LEVEL environment variable (default INFO).
"""

import logging
import os


def get_logger(name: str) -> logging.Logger:
    """
    Return a configured logger for the given module name.

    :param name: Logger name, typically __name__.
    :return: A logging.Logger configured per LOG_LEVEL from the environment.
    """
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        level=level,
    )
    return logging.getLogger(name)
