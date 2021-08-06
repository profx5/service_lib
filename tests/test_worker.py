import asyncio
import multiprocessing as mp
from queue import Empty as QueueEmpty
from typing import Any, Optional

import pytest

from service_lib.base_system import BaseSystem
from service_lib.logging import LoggerMixin
from service_lib.worker import Worker


class QueueSystem(BaseSystem, LoggerMixin):
    def __init__(self, queue: mp.Queue) -> None:
        self.queue = queue

    def safe_get(self) -> Optional[str]:
        try:
            return self.queue.get_nowait()
        except QueueEmpty:
            pass

    async def startup(self) -> None:
        self.logger.debug("Startup")

        if self.safe_get() == "startup_exception":
            raise Exception("Startup exception")

        self.queue.put("startup")

    async def run(self) -> None:
        self.logger.debug("Run")
        self.queue.put("run")
        stop_consuming = False
        try:
            while True:
                if not stop_consuming:
                    action = self.safe_get()
                    if action == "stop":
                        return
                    elif action == "runtime_exception":
                        raise Exception("Runtime exception")
                    elif action == "stop_consuming":
                        stop_consuming = True

                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            self.queue.put("cancelled_error")
            raise

    async def shutdown(self) -> None:
        action = self.safe_get()
        if action == "shutdown_exception":
            raise Exception("Shutdown exception")
        elif action == "shutdown_freeze":
            while True:
                await asyncio.sleep(1)

        self.logger.debug("Shutdown")
        self.queue.put("shutdown")


def main_for_test(queue: mp.Queue) -> None:
    system = QueueSystem(queue)
    worker = Worker(system, shutdown_timeout=0.5)

    worker.run()


if __name__ == "__main__":
    mp_queue = mp.Queue()
    mp_queue.put("runtime_exception")
    mp_queue.put("runtime_exception")
    main_for_test(mp_queue)


@pytest.fixture
def mp_context() -> Any:
    return mp.get_context("spawn")


@pytest.fixture
def mp_queue(mp_context: Any) -> mp.Queue:
    return mp_context.Queue()


@pytest.fixture
def mp_process(mp_context: Any, mp_queue: mp.Queue) -> mp.Queue:
    return mp_context.Process(target=main_for_test, args=(mp_queue,))


def test_simple_happy_path(mp_queue: mp.Queue, mp_process: mp.Process):
    mp_process.start()
    assert mp_queue.get(timeout=1) == "startup"
    assert mp_queue.get(timeout=1) == "run"

    mp_queue.put("stop")
    mp_process.join(timeout=1)
    assert mp_process.exitcode == 0
    assert mp_queue.get(timeout=1) == "shutdown"


def test_daemon_terminate(mp_queue: mp.Queue, mp_process: mp.Process):
    mp_process.start()
    assert mp_queue.get(timeout=1) == "startup"
    assert mp_queue.get(timeout=1) == "run"

    mp_process.terminate()
    mp_process.join(timeout=1)
    assert mp_process.exitcode == 0
    assert mp_queue.get(timeout=1) == "cancelled_error"
    assert mp_queue.get(timeout=1) == "shutdown"


def test_daemon_kill(mp_queue: mp.Queue, mp_process: mp.Process):
    mp_process.start()
    assert mp_queue.get(timeout=1) == "startup"
    assert mp_queue.get(timeout=1) == "run"

    mp_process.kill()
    mp_process.join(timeout=1)
    assert mp_process.exitcode == -9
    with pytest.raises(QueueEmpty):
        mp_queue.get(timeout=0.1)


def test_daemon_startup_exception(mp_queue: mp.Queue, mp_process: mp.Process):
    mp_queue.put("startup_exception")

    mp_process.start()

    mp_process.join(timeout=1)
    assert mp_process.exitcode == 1
    with pytest.raises(QueueEmpty):
        mp_queue.get(timeout=0.1)


def test_daemon_runtime_exception(mp_queue: mp.Queue, mp_process: mp.Process):
    mp_process.start()

    assert mp_queue.get(timeout=1) == "startup"
    assert mp_queue.get(timeout=1) == "run"

    mp_queue.put("runtime_exception")

    mp_process.join(timeout=1)
    assert mp_process.exitcode == 1
    assert mp_queue.get(timeout=1) == "shutdown"


def test_daemon_shutdown_exception(mp_queue: mp.Queue, mp_process: mp.Process):
    mp_process.start()

    assert mp_queue.get(timeout=1) == "startup"
    assert mp_queue.get(timeout=1) == "run"

    mp_queue.put("stop_consuming")
    mp_queue.put("shutdown_exception")

    mp_process.terminate()
    mp_process.join(timeout=1)
    mp_queue.get(timeout=0.1) == "canceled_error"
    assert mp_process.exitcode == 1


def test_daemon_freeze_on_shutdown(mp_queue: mp.Queue, mp_process: mp.Process):
    mp_process.start()

    assert mp_queue.get(timeout=1) == "startup"
    assert mp_queue.get(timeout=1) == "run"

    mp_queue.put("stop_consuming")
    mp_queue.put("shutdown_freeze")

    mp_process.terminate()
    mp_process.join(timeout=1)
    assert mp_process.exitcode == 1
    mp_queue.get(timeout=0.1) == "canceled_error"
