from typing import Any

from redis import asyncio as aioredis

from sifthub.serializer.interfaces import BaseSerializer


class RedisSerializerStore:

    def __init__(self, serializer: BaseSerializer, redis: aioredis.Redis):
        self.serializer = serializer
        self.redis = redis

    async def set_ex(self, key: str, expiry: int, value: Any):
        if value is None:
            return
        await self.redis.setex(key, expiry, await self.serializer.dumps(value))

    async def get_by_key(self, key: str):
        value = await self.redis.get(key)
        return None if value is None else await self.serializer.loads(value)

    async def remove_by_key(self, key: str):
        await self.redis.delete(key)

    async def set(self, key: str, hash_key: str, value: Any):
        if value is None:
            return
        await self.redis.hset(key, hash_key, await self.serializer.dumps(value))

    async def get(self, key: str, hash_key: str) -> Any:
        value = await self.redis.hget(key, hash_key)
        return None if value is None else await self.serializer.loads(value)
