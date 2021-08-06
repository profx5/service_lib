from typing import Any, Awaitable, Callable, Optional, TypeVar

import aiokafka
from pydantic import BaseModel

from service_lib.state import State

KT = TypeVar("KT", covariant=True)
VT = TypeVar("VT", covariant=True)

HandlerType = Callable[[KT, VT, State], Awaitable[Any]]
KeyParserType = Callable[[bytes], KT]
ValueParserType = Callable[[bytes], VT]


class RawRecord(aiokafka.ConsumerRecord[bytes, bytes]):  # type: ignore
    pass


class RecordHeaders(BaseModel):
    origin_topic: Optional[str]
    exception_info: Optional[str]
    occurred_at: Optional[int]

    class Config:
        extra = "allow"

    def prepare(self) -> list[tuple[str, bytes]]:
        return [(k, str(v).encode("utf-8")) for k, v in self.dict().items()]
