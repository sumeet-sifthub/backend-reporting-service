import asyncio
from typing import Any

import msgpack

from sifthub.serializer.interfaces import BaseSerializer


class MsgPackSerializer(BaseSerializer):
    async def dumps(self, value: Any) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, msgpack.packb, value)

    async def loads(self, value: bytes) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, msgpack.unpackb, value)
