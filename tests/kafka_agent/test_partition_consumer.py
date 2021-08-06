import asyncio
from typing import Any, Mapping
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from aiokafka import ConsumerRecord, ConsumerStoppedError
from kafka.structs import TopicPartition

from service_lib.kafka_agent.partition_consumer import PartitionConsumer
from service_lib.kafka_agent.retry_policy import no_retry_policy
from service_lib.kafka_agent.subscription import Subscription
from service_lib.state import State
from tests.kafka_agent.shared import (
    DLT_TOPIC,
    FIRST_RETRY_TOPIC,
    RETRY_POLICY,
    SECOND_RETRY_TOPIC,
    TOPIC,
    RetryableException,
)


@pytest.fixture
def mock_consumer():
    with patch("aiokafka.AIOKafkaConsumer", autospec=True, _records=[]) as mock:

        async def getone(*args, **kwargs):
            try:
                return mock._records.pop(0)
            except IndexError:
                raise ConsumerStoppedError()

        mock.getone = getone

        yield mock


@pytest.fixture
def mock_producer():
    with patch("aiokafka.AIOKafkaProducer", autospec=True) as mock:
        yield mock


@pytest.fixture
def handler():
    return AsyncMock()


@pytest.fixture
def subscription(handler) -> Subscription:
    return Subscription(
        topic_name=TOPIC, handler=handler, service_name="test_agent", retry_policy=RETRY_POLICY, timeout=1
    )


@pytest.fixture
def consumer(request, mock_consumer, mock_producer, subscription: Subscription) -> PartitionConsumer:
    topic_marker = request.node.get_closest_marker("consumer_topic")
    if topic_marker is None:
        topic = TOPIC
    else:
        topic = topic_marker.args[0]

    state = State()

    return PartitionConsumer(
        partition=TopicPartition(topic, 0),
        subscription=subscription,
        kafka_consumer=mock_consumer,
        kafka_producer=mock_producer,
        state=state,
    )


def add_record(
    mock_consumer: MagicMock, topic: str, key: bytes, value: bytes, offset: int = 0, headers: Mapping[str, Any] = None
) -> None:
    mock_consumer._records.append(
        ConsumerRecord(
            topic=topic,
            partition=0,
            offset=offset,
            timestamp=0,
            timestamp_type=0,
            key=key,
            value=value,
            checksum=0,
            serialized_key_size=0,
            serialized_value_size=0,
            headers=headers or {},
        )
    )


@pytest.mark.asyncio
async def test_process_message_successfully(mock_consumer, consumer: PartitionConsumer) -> None:
    add_record(mock_consumer, TOPIC, b"key1", b"value1", 0)
    add_record(mock_consumer, TOPIC, b"key2", b"value2", 1)
    await consumer.run()

    consumer.subscription.handler.assert_has_awaits(
        [
            call(b"key1", b"value1", consumer.state),
            call(b"key2", b"value2", consumer.state),
        ]
    )
    mock_consumer.commit.assert_has_awaits(
        [
            call(offsets={TopicPartition(topic=TOPIC, partition=0): 1}),
            call(offsets={TopicPartition(topic=TOPIC, partition=0): 2}),
        ]
    )


@pytest.mark.asyncio
async def test_process_message_error_no_retyr_policy(mock_consumer, consumer: PartitionConsumer, mock_producer) -> None:
    consumer.subscription.retry_policy = no_retry_policy
    consumer.subscription.handler.side_effect = Exception()
    add_record(mock_consumer, TOPIC, b"key1", b"value1")
    await consumer.run()

    mock_producer.send_and_wait.assert_not_called()
    mock_consumer.commit.assert_awaited_once_with(offsets={TopicPartition(topic=TOPIC, partition=0): 1})


@pytest.mark.asyncio
@pytest.mark.freeze_time("2021-07-13")
async def test_process_message_error_not_retryable(mock_consumer, consumer: PartitionConsumer, mock_producer) -> None:
    consumer.subscription.handler.side_effect = Exception()
    add_record(mock_consumer, TOPIC, b"key1", b"value1")
    await consumer.run()

    mock_producer.send_and_wait.assert_awaited_once_with(
        topic=DLT_TOPIC,
        value=b"value1",
        key=b"key1",
        headers=[
            ("origin_topic", TOPIC.encode("utf8")),
            ("exception_info", b"Exception()"),
            ("occurred_at", b"1626134400"),
        ],
    )
    mock_consumer.commit.assert_awaited_once_with(offsets={TopicPartition(topic=TOPIC, partition=0): 1})


@pytest.mark.asyncio
@pytest.mark.freeze_time("2021-07-13")
async def test_process_message_error_send_to_retry_topic(
    mock_consumer, consumer: PartitionConsumer, mock_producer
) -> None:
    consumer.subscription.handler.side_effect = RetryableException()
    add_record(mock_consumer, TOPIC, b"key1", b"value1")
    await consumer.run()

    mock_producer.send_and_wait.assert_awaited_once_with(
        topic=FIRST_RETRY_TOPIC,
        value=b"value1",
        key=b"key1",
        headers=[
            ("origin_topic", TOPIC.encode("utf8")),
            ("exception_info", b"RetryableException()"),
            ("occurred_at", b"1626134400"),
        ],
    )
    mock_consumer.commit.assert_awaited_once_with(offsets={TopicPartition(topic=TOPIC, partition=0): 1})


@pytest.mark.asyncio
@pytest.mark.consumer_topic(FIRST_RETRY_TOPIC)
async def test_process_retry_message_successfully(mock_consumer, consumer: PartitionConsumer) -> None:
    add_record(mock_consumer, FIRST_RETRY_TOPIC, b"key1", b"value1", 0)
    add_record(mock_consumer, FIRST_RETRY_TOPIC, b"key2", b"value2", 1)
    await consumer.run()

    consumer.subscription.handler.assert_has_awaits(
        [
            call(b"key1", b"value1", consumer.state),
            call(b"key2", b"value2", consumer.state),
        ]
    )
    mock_consumer.commit.assert_has_awaits(
        [
            call(offsets={TopicPartition(topic=FIRST_RETRY_TOPIC, partition=0): 1}),
            call(offsets={TopicPartition(topic=FIRST_RETRY_TOPIC, partition=0): 2}),
        ]
    )


@pytest.mark.asyncio
@pytest.mark.freeze_time("2021-07-13")
@pytest.mark.consumer_topic(FIRST_RETRY_TOPIC)
async def test_process_retry_send_to_dlt(mock_consumer, mock_producer, consumer: PartitionConsumer) -> None:
    consumer.subscription.handler.side_effect = Exception("foo")
    add_record(mock_consumer, FIRST_RETRY_TOPIC, b"key1", b"value1")
    await consumer.run()

    mock_producer.send_and_wait.assert_awaited_once_with(
        topic=DLT_TOPIC,
        value=b"value1",
        key=b"key1",
        headers=[
            ("origin_topic", FIRST_RETRY_TOPIC.encode("utf8")),
            ("exception_info", b"Exception('foo')"),
            ("occurred_at", b"1626134400"),
        ],
    )


@pytest.mark.asyncio
@pytest.mark.freeze_time("2021-07-13")
@pytest.mark.consumer_topic(FIRST_RETRY_TOPIC)
async def test_process_retry_send_to_next_backoff(mock_consumer, mock_producer, consumer: PartitionConsumer) -> None:
    consumer.subscription.handler.side_effect = RetryableException()
    add_record(mock_consumer, FIRST_RETRY_TOPIC, b"key1", b"value1")
    await consumer.run()

    mock_producer.send_and_wait.assert_awaited_once_with(
        topic=SECOND_RETRY_TOPIC,
        value=b"value1",
        key=b"key1",
        headers=[
            ("origin_topic", FIRST_RETRY_TOPIC.encode("utf8")),
            ("exception_info", b"RetryableException()"),
            ("occurred_at", b"1626134400"),
        ],
    )


@pytest.mark.asyncio
@pytest.mark.freeze_time("2021-07-13")
@pytest.mark.consumer_topic(SECOND_RETRY_TOPIC)
async def test_process_retry_last_retry_to_dlt(mock_consumer, mock_producer, consumer: PartitionConsumer) -> None:
    consumer.subscription.handler.side_effect = RetryableException()
    add_record(mock_consumer, SECOND_RETRY_TOPIC, b"key1", b"value1")
    await consumer.run()

    mock_producer.send_and_wait.assert_awaited_once_with(
        topic=DLT_TOPIC,
        value=b"value1",
        key=b"key1",
        headers=[
            ("origin_topic", SECOND_RETRY_TOPIC.encode("utf8")),
            ("exception_info", b"RetryableException()"),
            ("occurred_at", b"1626134400"),
        ],
    )


@pytest.mark.asyncio
@pytest.mark.freeze_time("2021-07-13")
@pytest.mark.consumer_topic(FIRST_RETRY_TOPIC)
async def test_process_retry_sleep_until(mock_consumer, mock_producer, consumer: PartitionConsumer) -> None:
    # sleep mock freezes ipdb.set_trace, use pdb instead
    with patch.object(asyncio, "sleep") as sleep_mock:
        add_record(
            mock_consumer,
            FIRST_RETRY_TOPIC,
            b"key1",
            b"value1",
            headers=[("occurred_at", b"1626134400")],
        )
        await consumer.run()
        sleep_mock.assert_awaited_once_with(1.0)
