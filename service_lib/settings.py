from typing import Optional

from pydantic import BaseSettings as PydanticBaseSettings

from service_lib.logging import LoggerSettings


class BaseSettings(PydanticBaseSettings):
    logging: LoggerSettings = LoggerSettings()
    sentry_dsn: Optional[str] = None
    environment: str = "development"
