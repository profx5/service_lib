import sentry_sdk
from pydantic import BaseSettings

from service_lib.logging import setup_logging


def setup_sentry(dsn: str, environment: str) -> None:
    sentry_sdk.init(dsn=dsn, environment=environment)


def prepare_infrastructure(settings: BaseSettings) -> None:
    if logging := getattr(settings, "logging", None):
        setup_logging(logging)

    if sentry_dsn := getattr(settings, "sentry_dsn", None):
        environment = getattr(settings, "environment", None)
        setup_sentry(sentry_dsn, environment)
