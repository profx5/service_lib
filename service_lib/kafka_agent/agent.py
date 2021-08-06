import asyncio
from dataclasses import dataclass
from typing import Any, Generic, Optional

import aiokafka

from service_lib.base_system import BaseSystem
from service_lib.kafka_agent.errors import AgentAlreadyStarted, AlreadySubscribed, UnknownTopic
from service_lib.kafka_agent.metrics_store import metrics_store
from service_lib.kafka_agent.partition_consumer import PartitionConsumer
from service_lib.kafka_agent.rebalance_listener import AgentRebalanceListener
from service_lib.kafka_agent.retry_policy import RetryPolicy, default_retry_policy
from service_lib.kafka_agent.subscription import Subscription
from service_lib.kafka_agent.types import KT, VT, HandlerType, KeyParserType, ValueParserType
from service_lib.logging import LoggerMixin
from service_lib.settings import BaseSettings
from service_lib.state_manager import StateManager


class AgentSettings(BaseSettings):
    kafka_servers: list[str]


@dataclass
class Consumer(Generic[KT, VT]):
    partition_consumer: PartitionConsumer[KT, VT]
    task: asyncio.Task[None]


class Agent(BaseSystem, LoggerMixin):
    def __init__(self, state_manager: StateManager, settings: AgentSettings, service_name: str) -> None:
        self.settings = settings
        self.state_manager = state_manager
        self.service_name = service_name

        self.started = False

        self.topic_to_subscription_map: dict[str, Subscription[Any, Any]] = {}
        self.partition_consumer_map: dict[aiokafka.TopicPartition, Consumer[Any, Any]] = {}

        self.stop = asyncio.Event()

    def add_subscription(
        self,
        topic: str,
        handler: HandlerType[KT, VT],
        timeout: int = 10,
        retry_policy: RetryPolicy = default_retry_policy,
        parse_key: Optional[KeyParserType[KT]] = None,
        parse_value: Optional[ValueParserType[KT]] = None,
    ) -> Subscription[KT, VT]:
        if self.started:
            raise AgentAlreadyStarted()

        if self.topic_to_subscription_map.get(topic) is not None:
            raise AlreadySubscribed(topic)

        subscription: Subscription[Any, Any] = Subscription(
            topic_name=topic,
            handler=handler,
            timeout=timeout,
            retry_policy=retry_policy,
            service_name=self.service_name,
            parse_key=parse_key,
            parse_value=parse_value,
        )

        for topic in subscription.all_topics(with_dlt=False):
            self.topic_to_subscription_map[topic] = subscription

        return subscription

    async def startup(self) -> None:
        self.logger.info("Starting up Agent")

        await self.state_manager.startup()

        self.kafka_consumer = aiokafka.AIOKafkaConsumer(
            bootstrap_servers=self.settings.kafka_servers, group_id=self.service_name, enable_auto_commit=False
        )
        self.kafka_producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.settings.kafka_servers, client_id=self.service_name
        )
        await self.kafka_producer.start()
        await self.kafka_consumer.start()

        self.started = True
        self.logger.info("Agent started up")

    async def shutdown(self) -> None:
        self.stop.set()
        self.logger.info("Shutting down Agent")

        await self.kafka_consumer.stop()
        await self.kafka_producer.stop()

        await self.state_manager.shutdown()
        self.logger.info("Agent shut down")

    def add_partition_consumer(self, partition: aiokafka.TopicPartition) -> None:
        self.logger.info(f"Adding consumer to partition {partition}")

        if (consumer := self.partition_consumer_map.get(partition)) is not None:
            self.logger.warning(f"Partition {partition} already had consumer {consumer}, removing")
            self.remove_partition_consumer(partition)

        try:
            subscription = self.topic_to_subscription_map[partition.topic]
        except KeyError:
            raise UnknownTopic(partition.topic)

        partition_consumer = PartitionConsumer(
            partition=partition,
            subscription=subscription,
            kafka_consumer=self.kafka_consumer,
            kafka_producer=self.kafka_producer,
            state=self.state_manager.state,
        )
        # TODO: add_done_callback for consumer finalization
        task = asyncio.create_task(
            partition_consumer.run(), name=f"TopicConsumer(topic={partition.topic}, partition={partition.partition})"
        )
        self.partition_consumer_map[partition] = Consumer(partition_consumer=partition_consumer, task=task)
        self.logger.info(f"Created consumer for partiton {partition}")
        metrics_store.partition_consumer_created.inc()

    def remove_partition_consumer(self, partition: aiokafka.TopicPartition) -> None:
        self.logger.info(f"Removing consumer for partiton {partition}")

        if (consumer := self.partition_consumer_map.get(partition)) is not None:
            consumer.task.cancel()
            del self.partition_consumer_map[partition]
            self.logger.info(f"Consumer for partiton {partition} cancelled and scheduled for remove")
            metrics_store.partition_consumer_removed.inc()

    @property
    def _topics(self) -> list[str]:
        return list(self.topic_to_subscription_map.keys())

    async def run(self) -> None:
        rebalance_listener = AgentRebalanceListener(self)

        self.kafka_consumer.subscribe(topics=self._topics, listener=rebalance_listener)
        self.logger.info(f"Subscribed to topic(s) {self._topics}")

        await self.stop.wait()
