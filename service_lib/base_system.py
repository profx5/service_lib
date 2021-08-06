import abc

from service_lib.lifecycle import Lifecycle


class BaseSystem(Lifecycle, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def run(self) -> None:
        pass
