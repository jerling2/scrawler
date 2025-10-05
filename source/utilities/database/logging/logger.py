from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from functools import wraps
from typing import TextIO, Literal, Optional, Any
import sys
import os
import logging


@dataclass(frozen=True)
class DBLoggerConfig:
    formatter: logging.Formatter=logging.Formatter(
        fmt='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %I:%M %p'
    )
    master_level: int=logging.DEBUG
    console_stream: TextIO=sys.stdout
    console_level: int=logging.DEBUG
    file_path: Path=Path(os.getenv("LOG_DIRECTORY")) / 'db.log'
    file_mode: Literal['a', 'w', 'x']='a'
    file_level: int=logging.DEBUG


def get_database_logger(config: DBLoggerConfig=DBLoggerConfig()) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(config.master_level)
    if not logger.handlers:
        # prevent adding duplicate handlers to this logger.
        console_handler = logging.StreamHandler(config.console_stream)
        console_handler.setLevel(config.console_level)
        console_handler.setFormatter(config.formatter)
        file_handler = logging.FileHandler(config.file_path, config.file_mode, encoding='utf-8')
        file_handler.setLevel(config.file_level)
        file_handler.setFormatter(config.formatter)
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
    return logger


logger = get_database_logger()


def log_errors(
    operation_name: str,
    logger_func: logging.Logger=logger,
    reraise: bool = True,
    return_on_error: Optional[Any] = None
):
    """Wraps database operations with error logging and optional exception handling.
    
    Args:
    - operation_name (str): Name of the operation being logged.
    - logger_func (Logger): Logger instance to use. Defaults to module logger.
    - reraise (bool): If True, re-raises exceptions after logging. Defaults to True.
    - return_on_error (Optional[Any]): Value to return if exception occurs and reraise is False.
        Defaults to None.
    
    Returns:
    - callable: Decorated function with error logging capability.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                logger_func.error(
                    f"Error in '{operation_name}' for '{self.config.database}': "
                    f"{type(e).__name__}: {str(e).strip()}"
                )
                if reraise:
                    raise
            return return_on_error
        return wrapper
    return decorator