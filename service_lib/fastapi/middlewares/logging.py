from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


class LoggingMiddleware(BaseHTTPMiddleware):
    """
    Обогащает контекст loguru
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        context = {
            "method": request.method,
            "path": request.url.path,
            "query": request.url.query,
            "user_agent": request.headers.get("User-Agent"),
        }

        if request_id := request.headers.get("X-Request-ID"):
            context["request_id"] = request_id

        with logger.contextualize(**context):
            try:
                return await call_next(request)
            except Exception as e:
                logger.exception(e)
                raise
