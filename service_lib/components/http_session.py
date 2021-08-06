from typing import Optional

import aiohttp

from service_lib.components.base import BaseComponent
from service_lib.state import State


class AIOHTTPSessionComponent(BaseComponent):
    session: Optional[aiohttp.ClientSession]

    async def startup(self, state: State) -> State:
        self.session = aiohttp.ClientSession()
        state.session = self
        return state

    async def shutdown(self) -> None:
        if self.session:
            await self.session.close()

    async def healthcheck(self) -> None:
        assert self.session is not None, "Session is not initialized"
