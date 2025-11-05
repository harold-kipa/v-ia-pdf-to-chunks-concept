import logging
from logging.handlers import RotatingFileHandler

def get_logger(name: str = "app"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # handler rotativo de 5 MB, mantiene 3 backups
    handler = RotatingFileHandler(
        "app.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    # evita agregar handlers duplicados si se llama varias veces
    if not logger.handlers:
        logger.addHandler(handler)

    return logger
