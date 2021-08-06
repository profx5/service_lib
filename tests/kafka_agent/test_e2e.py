import asyncio
import os
import pathlib
import uuid
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, Tuple
from unittest.mock import AsyncMock, Mock, call

import aiokafka
import pytest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from tenacity import AsyncRetrying, Retrying, retry_if_exception_type, stop_after_attempt, wait_fixed
from testcontainers.compose import DockerCompose

from service_lib.kafka_agent import Agent, AgentSettings, no_retry_policy
from service_lib.state_manager import StateManager
from tests.kafka_agent.shared import (
    DLT_TOPIC,
    FIRST_RETRY_TOPIC,
    RETRY_POLICY,
    SECOND_RETRY_TOPIC,
    TOPIC,
    RetryableException,
)

KAFKA_STARTUP_TIMEOUT = 30
KAFKA_SERVERS = ["localhost:9092"]

QUEUE_TIMEOUT = 10
GETONE_TIMEOUT = 10


@pytest.fixture(scope="module")
def kafka_container():
    if os.environ.get("USE_LOCAL_KAFKA"):
        yield
        return

    with DockerCompose(pathlib.Path(__file__).parent.absolute(), compose_file_name="docker-compose.yaml") as compose:
        for attempt in Retrying(
            stop=stop_after_attempt(30), wait=wait_fixed(1), retry=retry_if_exception_type(AssertionError)
        ):
            with attempt:
                stdout, _ = compose.get_logs()

                assert (
                    b"INFO [Controller id=1] Processing automatic preferred replica leader election (kafka.controller.KafkaController)"
                    in stdout
                ), "Kafka not started"

        yield


@pytest.fixture
def agent(kafka_container) -> Agent:
    settings = AgentSettings(kafka_servers=KAFKA_SERVERS)
    state_manager = StateManager([], settings)

    system = Agent(state_manager, settings, service_name="test_agent")

    return system


@pytest.fixture
async def kafka_producer(kafka_container) -> aiokafka.AIOKafkaProducer:
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers=KAFKA_SERVERS)
    await producer.start()

    yield producer

    await producer.stop()


@pytest.fixture
async def kafka_consumer(kafka_container) -> aiokafka.AIOKafkaConsumer:
    producer = aiokafka.AIOKafkaConsumer(bootstrap_servers=KAFKA_SERVERS)
    await producer.start()

    yield producer

    await producer.stop()


@pytest.fixture
def admin_client(kafka_container) -> KafkaAdminClient:
    return KafkaAdminClient(bootstrap_servers=KAFKA_SERVERS)


@asynccontextmanager
async def start_agent(agent: Agent):
    await agent.startup()
    asyncio.create_task(agent.run())

    try:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(20), wait=wait_fixed(1), retry=retry_if_exception_type(AssertionError)
        ):
            with attempt:
                assert agent.partition_consumer_map != {}, "Partitions not assigned"

        yield agent
    finally:
        await agent.shutdown()


def recreate_topic(admin_client: KafkaAdminClient, name: str) -> None:
    try:
        admin_client.delete_topics([name])
    except UnknownTopicOrPartitionError:
        print(f"can't delete topic {name}")

    for attempt in Retrying(
        stop=stop_after_attempt(30),
        wait=wait_fixed(1),
        retry=retry_if_exception_type(TopicAlreadyExistsError),
    ):
        with attempt:
            admin_client.create_topics(
                [
                    NewTopic(name=name, num_partitions=1, replication_factor=1),
                ]
            )


@pytest.fixture
def topics(admin_client: KafkaAdminClient):
    for topic in [TOPIC, FIRST_RETRY_TOPIC, SECOND_RETRY_TOPIC, DLT_TOPIC]:
        recreate_topic(admin_client, topic)


QUEUE_HANDLER_TYPE = Tuple[asyncio.Queue[AsyncMock], Callable[..., Awaitable[None]], Mock]


@pytest.fixture
def queue_handler() -> QUEUE_HANDLER_TYPE:
    queue = asyncio.Queue()
    mock = Mock()

    async def handler(*args, **kwargs):
        try:
            mock(*args, **kwargs)
        finally:
            await queue.put(mock)

    return (queue, handler, mock)


@pytest.fixture
def random_key_value() -> Tuple[str, str]:
    return uuid.uuid4().hex.encode("utf8"), uuid.uuid4().hex.encode("utf8")


def raise_once(exc: Exception) -> Callable[..., Any]:
    called = False

    def f(*args, **kwargs):
        nonlocal called
        if not called:
            called = True
            raise exc

    return f


@pytest.mark.slow
@pytest.mark.asyncio
@pytest.mark.usefixtures("topics")
async def test_happy_path(
    agent: Agent,
    kafka_producer: aiokafka.AIOKafkaProducer,
    queue_handler: QUEUE_HANDLER_TYPE,
    random_key_value: Tuple[str, str],
):
    queue, handler, _ = queue_handler
    agent.add_subscription(TOPIC, handler, retry_policy=RETRY_POLICY)

    key, value = random_key_value
    async with start_agent(agent):
        await kafka_producer.send_and_wait(TOPIC, key=key, value=value)

        mock = await asyncio.wait_for(queue.get(), timeout=QUEUE_TIMEOUT)
        mock.assert_called_once_with(key, value, agent.state_manager.state)


@pytest.mark.slow
@pytest.mark.asyncio
@pytest.mark.usefixtures("topics")
async def test_processing_non_retryable(
    agent: Agent,
    kafka_producer: aiokafka.AIOKafkaProducer,
    kafka_consumer: aiokafka.AIOKafkaConsumer,
    queue_handler: QUEUE_HANDLER_TYPE,
    random_key_value: Tuple[str, str],
):
    queue, handler, mock = queue_handler
    agent.add_subscription(TOPIC, handler, retry_policy=RETRY_POLICY)
    mock.side_effect = Exception("foo")

    kafka_consumer.subscribe([DLT_TOPIC])

    key, value = random_key_value
    async with start_agent(agent):
        await kafka_producer.send_and_wait(TOPIC, key=key, value=value)

        await asyncio.wait_for(queue.get(), timeout=QUEUE_TIMEOUT)

    record = await asyncio.wait_for(kafka_consumer.getone(), timeout=GETONE_TIMEOUT)
    assert (record.key, record.value, record.headers) == (
        key,
        value,
        (
            ("origin_topic", b"test_topic"),
            ("exception_info", b"Exception('foo')"),
            ("occurred_at", record.headers[2][1]),
        ),
    )


@pytest.mark.slow
@pytest.mark.asyncio
@pytest.mark.usefixtures("topics")
async def test_processing_retryable_to_dlt(
    agent: Agent,
    kafka_producer: aiokafka.AIOKafkaProducer,
    kafka_consumer: aiokafka.AIOKafkaConsumer,
    queue_handler: QUEUE_HANDLER_TYPE,
    random_key_value: Tuple[str, str],
):
    queue, handler, mock = queue_handler
    agent.add_subscription(TOPIC, handler, retry_policy=RETRY_POLICY)
    mock.side_effect = RetryableException()

    kafka_consumer.subscribe([FIRST_RETRY_TOPIC, SECOND_RETRY_TOPIC, DLT_TOPIC])

    key, value = random_key_value
    async with start_agent(agent):
        await kafka_producer.send_and_wait(TOPIC, key=key, value=value)

        mock = await asyncio.wait_for(queue.get(), timeout=QUEUE_TIMEOUT)

        record = await asyncio.wait_for(kafka_consumer.getone(), timeout=GETONE_TIMEOUT)
        assert (record.key, record.value, record.headers) == (
            key,
            value,
            (
                ("origin_topic", TOPIC.encode("utf8")),
                ("exception_info", b"RetryableException()"),
                ("occurred_at", record.headers[2][1]),
            ),
        )

        record = await asyncio.wait_for(kafka_consumer.getone(), timeout=GETONE_TIMEOUT)
        assert (record.key, record.value, record.headers) == (
            key,
            value,
            (
                ("origin_topic", FIRST_RETRY_TOPIC.encode("utf8")),
                ("exception_info", b"RetryableException()"),
                ("occurred_at", record.headers[2][1]),
            ),
        )

        record = await asyncio.wait_for(kafka_consumer.getone(), timeout=GETONE_TIMEOUT)
        assert (record.key, record.value, record.headers) == (
            key,
            value,
            (
                ("origin_topic", SECOND_RETRY_TOPIC.encode("utf8")),
                ("exception_info", b"RetryableException()"),
                ("occurred_at", record.headers[2][1]),
            ),
        )

        assert mock.call_args_list == [call(key, value, agent.state_manager.state) for _ in range(3)]


@pytest.mark.slow
@pytest.mark.asyncio
@pytest.mark.usefixtures("topics")
async def test_processing_retryable_success_retry(
    agent: Agent,
    kafka_producer: aiokafka.AIOKafkaProducer,
    queue_handler: QUEUE_HANDLER_TYPE,
    random_key_value: Tuple[str, str],
):
    queue, handler, mock = queue_handler
    agent.add_subscription(TOPIC, handler, retry_policy=RETRY_POLICY)
    mock.side_effect = raise_once(RetryableException())

    key, value = random_key_value
    async with start_agent(agent):
        await kafka_producer.send_and_wait(TOPIC, key=key, value=value)

        await asyncio.wait_for(queue.get(), timeout=QUEUE_TIMEOUT)

        mock = await asyncio.wait_for(queue.get(), timeout=QUEUE_TIMEOUT)

        assert mock.call_args_list == [call(key, value, agent.state_manager.state) for _ in range(2)]


@pytest.mark.slow
@pytest.mark.asyncio
@pytest.mark.usefixtures("topics")
async def test_processing_no_retry_policy(
    agent: Agent,
    kafka_producer: aiokafka.AIOKafkaProducer,
    queue_handler: QUEUE_HANDLER_TYPE,
    random_key_value: Tuple[str, str],
):
    queue, handler, mock = queue_handler
    agent.add_subscription(TOPIC, handler, retry_policy=no_retry_policy)
    mock.side_effect = RetryableException()

    key, value = random_key_value
    async with start_agent(agent):
        await kafka_producer.send_and_wait(TOPIC, key=key, value=value)

        await asyncio.wait_for(queue.get(), timeout=QUEUE_TIMEOUT)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(queue.get(), timeout=QUEUE_TIMEOUT)
