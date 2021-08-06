from typing import Optional

import aioredis

from service_lib.components.base import BaseComponent
from service_lib.state import State


class RedisComponent(BaseComponent):
    redis: Optional[aioredis.ConnectionsPool]

    async def startup(self, state: State) -> State:
        self.redis = await aioredis.create_redis_pool(state.settings.redis_dsn)
        state.redis = self.redis
        return state

    async def healthcheck(self) -> None:
        assert self.redis is not None, "Redis is not initialized"
        await self.redis.ping()

    async def shutdown(self) -> None:
        if self.redis:
            self.redis.close()
            await self.redis.wait_closed()
