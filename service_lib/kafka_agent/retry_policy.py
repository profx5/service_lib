from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional, Sequence

from service_lib.kafka_agent.retryable_exceptions import retryable_exceptions
from service_lib.utils.functional import safe_get_by_idx


@dataclass
class Backoff:
    name: str
    delta: timedelta


@dataclass(frozen=True)
class RetryPolicy:
    backoffs: Sequence[Backoff]
    retryable_exceptions: Sequence[type[Exception]] = field(default_factory=lambda: retryable_exceptions)
    dlt_enabled: bool = field(default=True)

    def get_backoff(self, name: str) -> Backoff:
        for backoff in self.backoffs:
            if backoff.name == name:
                return backoff

        raise ValueError(f"Backoff with name={name} not found")

    def get_next_backoff(self, backoff: Backoff) -> Optional[Backoff]:
        try:
            idx = self.backoffs.index(backoff)
            return safe_get_by_idx(self.backoffs, idx + 1)
        except ValueError:
            return None

    @property
    def first_backoff(self) -> Optional[Backoff]:
        return safe_get_by_idx(self.backoffs, 0)

    def should_retry_exception(self, exception: Exception) -> bool:
        for e in self.retryable_exceptions:
            if isinstance(exception, e):
                return True

        return False


default_backoffs = [
    Backoff(name="1m", delta=timedelta(minutes=1)),
    Backoff(name="15m", delta=timedelta(minutes=15)),
    Backoff(name="1h", delta=timedelta(hours=1)),
]
default_retry_policy = RetryPolicy(
    backoffs=default_backoffs,
    retryable_exceptions=retryable_exceptions,
    dlt_enabled=True,
)

no_retry_policy = RetryPolicy(backoffs=[], retryable_exceptions=[], dlt_enabled=False)
