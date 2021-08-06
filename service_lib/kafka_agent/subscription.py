from dataclasses import dataclass
from typing import Generic, Optional

from service_lib.kafka_agent.retry_policy import Backoff, RetryPolicy
from service_lib.kafka_agent.types import KT, VT, HandlerType, KeyParserType, ValueParserType


@dataclass
class Subscription(Generic[KT, VT]):
    topic_name: str
    handler: HandlerType[KT, VT]
    retry_policy: RetryPolicy
    service_name: str
    timeout: int
    parse_key: Optional[KeyParserType[KT]] = None
    parse_value: Optional[ValueParserType[VT]] = None

    @property
    def dlt_topic(self) -> str:
        return f"{self.topic_name}__{self.service_name}__dlt"

    def topic_for_backoff(self, backoff: Backoff) -> str:
        return f"{self.topic_name}__{self.service_name}__{backoff.name}"

    def all_topics(self, with_dlt: bool = False) -> list[str]:
        topics = [self.topic_name]

        for backoff in self.retry_policy.backoffs:
            topics.append(self.topic_for_backoff(backoff))

        if with_dlt and self.retry_policy.dlt_enabled:
            topics.append(self.dlt_topic)

        return topics

    def get_backoff_for_topic(self, topic: str) -> Optional[Backoff]:
        for backoff in self.retry_policy.backoffs:
            if self.topic_for_backoff(backoff) == topic:
                return backoff

        return None
