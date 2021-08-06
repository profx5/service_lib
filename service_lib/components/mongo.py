from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import AnyUrl

from service_lib.components.base import BaseComponent
from service_lib.state import State


class MongoDsn(AnyUrl):
    allowed_schemes = {"mongodb"}


class MotorComponent(BaseComponent):
    mongo: Optional[AsyncIOMotorClient]

    async def startup(self, state: State) -> State:
        self.mongo = AsyncIOMotorClient(state.settings.mongo_dsn)
        state.mongo = self.mongo
        return state

    async def shutdown(self) -> None:
        if self.mongo:
            self.mongo.close()

    async def healthcheck(self) -> None:
        assert self.mongo is not None, "Mongo is not initialized"
        await self.mongo.server_info()
