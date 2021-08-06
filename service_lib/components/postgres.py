from contextlib import asynccontextmanager
from typing import Optional

from aiopg.sa import Engine, SAConnection, create_engine

from service_lib.components.base import BaseComponent
from service_lib.state import State


class AIOPostgresComponent(BaseComponent):
    engine: Optional[Engine]

    async def startup(self, state: State) -> State:
        self.engine = await create_engine(state.settings.postgres_dsn)
        state.db = self.engine
        return state

    async def healthcheck(self) -> None:
        async with self._get_connection() as conn:
            await conn.execute("select 1;")

    async def shutdown(self) -> None:
        if self.engine:
            self.engine.close()
            await self.engine.wait_closed()

    @asynccontextmanager
    async def _get_connection(self) -> SAConnection:
        assert self.engine is not None, "Engine is not initialized"
        async with self.engine.acquire() as connection:
            yield connection
