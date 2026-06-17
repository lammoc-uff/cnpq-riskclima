from pathlib import Path
import logging
from datetime import datetime
import os

def get_logger(name: str, log_dir: str = None):
    """
    Configure and return a logger instance for the project.
    """
    
    if log_dir is None:
        base = Path(__file__).resolve().parents[3]  # raiz do projeto
        log_dir = base / "logs"

    os.makedirs(log_dir, exist_ok=True)
    log_file = log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger
