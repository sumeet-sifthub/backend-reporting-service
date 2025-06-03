import abc
from typing import Any


class BaseSerializer(abc.ABC):

    @abc.abstractmethod
    async def dumps(self, value: Any) -> bytes:
        raise NotImplementedError

    @abc.abstractmethod
    async def loads(self, value: bytes) -> Any:
        raise NotImplementedError
