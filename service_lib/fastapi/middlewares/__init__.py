from typing import TYPE_CHECKING

from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from starlette_exporter import PrometheusMiddleware

from service_lib.fastapi.middlewares.logging import LoggingMiddleware

if TYPE_CHECKING:
    from fastapi import FastAPI

default_middlewares = [LoggingMiddleware, PrometheusMiddleware, SentryAsgiMiddleware]


def set_default_middlewares(app: "FastAPI") -> None:
    for middleware in default_middlewares:
        app.add_middleware(middleware)
