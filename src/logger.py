"""
Configuração centralizada de logging.
Todos os módulos importam get_logger() daqui.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logging(log_level: str = "INFO", log_dir: Path | None = None) -> None:
    """
    Configura o root logger com handlers para stdout e arquivo.
    Deve ser chamado uma vez no entry-point do pipeline.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
    ]

    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """Retorna logger com o nome dado (tipicamente __name__ do módulo)."""
    return logging.getLogger(name)
