from typing import Optional

import aiokafka

from service_lib.components.base import BaseComponent
from service_lib.state import State


class KafkaProducerComponent(BaseComponent):
    producer: Optional[aiokafka.AIOKafkaProducer]

    async def startup(self, state: State) -> State:
        self.producer = aiokafka.AIOKafkaProducer(bootstrap_servers=state.settings.kafka_servers)

        await self.producer.start()
        state.producer = self.producer
        return state

    async def shutdown(self) -> None:
        if self.producer:
            await self.producer.stop()

    async def healthcheck(self) -> None:
        assert self.producer is not None, "Producer is not initialized"
        await self.producer.send_and_wait("healthckeck", b"healthckeck-message")
