import logging
import sys
from typing import Union

import loguru  # for type hints
from loguru import logger
from pydantic import BaseModel


class InterceptLoguruHandler(logging.Handler):
    def __init__(self, stream: str = ""):
        del stream
        super().__init__()

    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists
        try:
            level: Union[str, int] = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back  # type: ignore
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


class LoggerSettings(BaseModel):
    json_enabled: bool = False
    level: str = "INFO"
    extra_context: bool = False


def setup_logging(settings: LoggerSettings) -> None:
    logging.basicConfig(handlers=[InterceptLoguruHandler()], level=0)

    if settings.json_enabled:
        logger.configure(
            handlers=[
                {
                    "sink": sys.stdout,
                    "serialize": True,
                    "colorize": False,
                    "level": settings.level,
                    "backtrace": False,
                }
            ],
        )
    else:
        format = (
            "<green>{time:YYYY-MM-DDTHH:mm:ss.SSS}</green> | <level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )
        if settings.extra_context:
            format += " | <level>{extra!s}</level>"

        logger.configure(
            handlers=[{"sink": sys.stdout, "level": settings.level, "backtrace": False, "format": format}],
        )


class LoggerMixin:
    @property
    def logger(self) -> "loguru.Logger":
        if not hasattr(self, "_logger"):
            self._logger = loguru.logger.bind(class_name=self.__class__.__name__)

        return self._logger
