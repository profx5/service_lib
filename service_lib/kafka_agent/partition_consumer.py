import asyncio
from datetime import datetime
from typing import Generic

import aiokafka
import sentry_sdk
from pydantic import ValidationError

from service_lib.kafka_agent.errors import ParseKeyValueError
from service_lib.kafka_agent.metrics_store import metrics_store
from service_lib.kafka_agent.retry_policy import Backoff
from service_lib.kafka_agent.subscription import Subscription
from service_lib.kafka_agent.types import KT, VT, RawRecord, RecordHeaders
from service_lib.logging import LoggerMixin
from service_lib.state import State
from service_lib.utils.asyncio import wait_until


class PartitionConsumer(LoggerMixin, Generic[KT, VT]):
    def __init__(
        self,
        partition: aiokafka.TopicPartition,
        subscription: Subscription[KT, VT],
        kafka_consumer: aiokafka.AIOKafkaConsumer,
        kafka_producer: aiokafka.AIOKafkaProducer,
        state: State,
    ) -> None:
        self.partition = partition
        self.subscription = subscription
        self.kafka_consumer = kafka_consumer
        self.kafka_producer = kafka_producer
        self.state = state

        self.current_backoff = self.subscription.get_backoff_for_topic(partition.topic)
        if self.current_backoff is None:
            self.next_backoff = self.subscription.retry_policy.first_backoff
        else:
            self.next_backoff = self.subscription.retry_policy.get_next_backoff(self.current_backoff)

        self._logger = self.logger.bind(topic=self.partition.topic, partition=self.partition.partition)

    def _log_got_message(self, record: RawRecord) -> None:
        self.logger.info(
            "Got message",
            topic=record.topic,
            offset=record.offset,
            key=record.key,
            value=record.value,
            headers=record.headers,
        )

    def _safe_parse_headers(self, record: RawRecord) -> RecordHeaders:
        try:
            prepared_headers = {item[0]: item[1].decode() for item in record.headers}
            return RecordHeaders(**prepared_headers)
        except (ValidationError, KeyError) as e:
            self.logger.error("Error while parsing headers", error=repr(e))
            return RecordHeaders()

    def _prepare_key_value(self, record: RawRecord) -> tuple[KT, VT]:
        try:
            if self.subscription.parse_key is not None:
                key = self.subscription.parse_key(record.key)
            else:
                key = record.key

            if self.subscription.parse_value is not None:
                value = self.subscription.parse_value(record.value)
            else:
                value = record.value

            return key, value
        except Exception:
            raise ParseKeyValueError()

    async def _send_to_retry_topic(self, record: RawRecord, backoff: Backoff, headers: RecordHeaders) -> None:
        await self.kafka_producer.send_and_wait(
            topic=self.subscription.topic_for_backoff(backoff),
            value=record.value,
            key=record.key,
            headers=headers.prepare(),
        )

    async def _send_to_dlt(self, record: RawRecord, headers: RecordHeaders) -> None:
        await self.kafka_producer.send_and_wait(
            topic=self.subscription.dlt_topic, value=record.value, key=record.key, headers=headers.prepare()
        )

    async def _run_handler(self, record: RawRecord) -> None:
        key, value = self._prepare_key_value(record)

        await asyncio.wait_for(self.subscription.handler(key, value, self.state), timeout=self.subscription.timeout)
        self.logger.info("Successfully processed")

    async def _process_exception(self, record: RawRecord, exception: Exception) -> None:
        self.logger.exception("Error while processing message", exc_info=Exception)
        sentry_sdk.capture_exception(exception)
        metrics_store.errors_counter.labels(topic=record.topic, error_class=exception.__class__.__name__).inc()

        headers = RecordHeaders(
            origin_topic=record.topic, exception_info=repr(exception), occurred_at=datetime.utcnow().timestamp()
        )

        if self.next_backoff is not None and self.subscription.retry_policy.should_retry_exception(exception):
            self.logger.info(f"Sending to {self.next_backoff.name} retry topic")
            await self._send_to_retry_topic(record, self.next_backoff, headers)
        elif self.subscription.retry_policy.dlt_enabled:
            self.logger.info("Sending to dlt")
            await self._send_to_dlt(record, headers)
        else:
            self.logger.info("Nothing to do with exception")

    async def _wait_for_backoff(self, record: RawRecord) -> None:
        if self.current_backoff:
            headers = self._safe_parse_headers(record)

            if headers.occurred_at is not None:
                occurred_at = datetime.fromtimestamp(headers.occurred_at)

                run_at = occurred_at + self.current_backoff.delta
                if run_at > datetime.utcnow():
                    self.logger.info(f"Waiting until {run_at.isoformat()}")
                    await wait_until(run_at)

    async def process_record(self, record: RawRecord) -> None:
        self._log_got_message(record)
        with (
            self.logger.contextualize(offset=record.offset),
            metrics_store.process_message_hist.labels(topic=record.topic).time(),
        ):
            try:
                if self.current_backoff is not None:
                    await self._wait_for_backoff(record)

                await self._run_handler(record)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self._process_exception(record=record, exception=e)

    async def commit(self, last_record: RawRecord) -> None:
        await self.kafka_consumer.commit(offsets={self.partition: last_record.offset + 1})

    async def run(self) -> None:
        while True:
            try:
                # TODO: add batch processing
                record = await self.kafka_consumer.getone(self.partition)
                await self.process_record(record)
                await self.commit(record)
            except asyncio.CancelledError:
                self.logger.warning("Got CancelledError")
                return
            except aiokafka.ConsumerStoppedError:
                self.logger.warning("Consumer stopped")
                return
            except Exception:
                self.logger.exception("Unexpected exception in PartitionConsumer")
                raise
