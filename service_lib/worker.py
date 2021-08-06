import asyncio
import signal
from typing import Optional

import prometheus_client

from service_lib.base_system import BaseSystem
from service_lib.logging import LoggerMixin


class Worker(LoggerMixin):
    PROMETHEUS_PORT = 9850

    """Воркер реагирующий на сигналы OS и реализующий graceful shutdown.

    Пример использования:

    class DummySystem(BaseSystem):
        async def startup(self):
            pass

        async def run(self):
            await asyncio.sleep(10)

        async def startup(self):
            pass

    system = DummySystem()
    worker = Worker(system)

    worker.run()
    """

    def __init__(self, system: BaseSystem, shutdown_timeout: int = 10, prometheus_server: bool = True) -> None:
        """
        Args:
            system: "система" которой будет управлять воркер
            shutdown_timeout: таймаут на выполнения shutdown
            prometheus_server: флаг для поднятия сервера prometheus
        """
        self.loop = asyncio.get_event_loop()
        self.system = system
        self.shutdown_timeout = shutdown_timeout
        self.prometheus_server = prometheus_server

    def run(self) -> None:
        self._set_signal_handlers()
        exitcode = None

        self.loop.run_until_complete(self.startup())
        try:
            self.loop.run_until_complete(self.system.run())
            self.logger.info("%s.run complete" % self.system.__class__.__name__)
        except asyncio.CancelledError:
            pass
        except Exception:
            self.logger.exception("Got error during runtime")
            exitcode = 1
        finally:
            self.loop.run_until_complete(self._graceful_shutdown(exitcode=exitcode))

    async def startup(self) -> None:
        self.logger.info("Worker startup")

        await self.system.startup()

        if self.prometheus_server:
            prometheus_client.start_http_server(port=self.PROMETHEUS_PORT)

    async def shutdown(self) -> None:
        self.logger.info("Worker shutdown")

        await self.system.shutdown()

    async def _graceful_shutdown(self, exitcode: Optional[int] = None) -> None:
        try:
            await asyncio.wait_for(self.shutdown(), timeout=self.shutdown_timeout)
        except TimeoutError:
            self.logger.error("Shutdown timeout error, timeout=%s", self.shutdown_timeout)

        self.logger.info("Cancelling pending tasks")
        tasks = asyncio.all_tasks(loop=self.loop)
        current_task = asyncio.current_task(self.loop)
        if current_task:
            tasks.remove(current_task)

        for t in tasks:
            self.logger.debug("Cancelling task: %s", t)
            t.cancel()

        if exitcode:
            exit(exitcode)

    def _signal_handler(self) -> None:
        self.logger.error("Got stop signal")
        self._clear_signal_handlers()
        self.loop.create_task(self._graceful_shutdown())

    def _clear_signal_handlers(self) -> None:
        self.loop.remove_signal_handler(signal.SIGINT)
        self.loop.remove_signal_handler(signal.SIGTERM)

    def _set_signal_handlers(self) -> None:
        self.loop.add_signal_handler(signal.SIGINT, self._signal_handler)
        self.loop.add_signal_handler(signal.SIGTERM, self._signal_handler)
