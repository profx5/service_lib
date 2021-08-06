import abc


class Lifecycle(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def startup(self) -> None:
        pass

    @abc.abstractmethod
    async def shutdown(self) -> None:
        pass
