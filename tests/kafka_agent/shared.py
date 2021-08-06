from datetime import timedelta

from service_lib.kafka_agent import Backoff, RetryPolicy

TOPIC = "test_topic"
FIRST_RETRY_TOPIC = "test_topic__test_agent__1s"
SECOND_RETRY_TOPIC = "test_topic__test_agent__3s"
DLT_TOPIC = "test_topic__test_agent__dlt"


class RetryableException(Exception):
    pass


RETRY_POLICY = RetryPolicy(
    backoffs=[Backoff(name="1s", delta=timedelta(seconds=1)), Backoff(name="3s", delta=timedelta(seconds=3))],
    retryable_exceptions=[RetryableException],
    dlt_enabled=True,
)
