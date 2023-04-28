import logging
import logging.handlers
from collections.abc import Iterable

"""
Настройка logger
"""


def _init_logger(name, filename: str | None = None):
    directory_path = "logs/"
    logger = logging.getLogger(name)
    format = '%(asctime)s :: %(name)s:%(lineno)s :: %(levelname)s :: %(message)s'
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter(format))
    sh.setLevel(logging.DEBUG)
    fh = logging.handlers.RotatingFileHandler(
        filename='logs/test.log' if not filename else directory_path + filename,
        maxBytes=1250000,
        backupCount=1
    )
    fh.setFormatter(logging.Formatter(format))
    fh.setLevel(logging.DEBUG)
    logger.addHandler(sh)
    logger.addHandler(fh)

    return logger
