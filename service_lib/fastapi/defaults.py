from typing import List

from fastapi import FastAPI
from starlette_exporter import handle_metrics

from service_lib.components.base import BaseComponent
from service_lib.fastapi.middlewares import set_default_middlewares
from service_lib.settings import BaseSettings
from service_lib.state_manager import FastAPIStateManager
from service_lib.utils.infrastructure import prepare_infrastructure


def set_service_routes(app: FastAPI) -> None:
    app.add_route("/metrics", handle_metrics)


def set_defaults(app: FastAPI) -> None:
    set_default_middlewares(app)
    set_service_routes(app)


def create_default_app(settings: BaseSettings, components: List[BaseComponent], title: str) -> FastAPI:
    prepare_infrastructure(settings)
    state_manager = FastAPIStateManager(settings=settings, components=components)

    app = FastAPI(
        title=title,
    )
    state_manager.set_fastapi_startup_hook(app)
    state_manager.set_fastapi_shutdown_hook(app)
    set_defaults(app)
    return app
