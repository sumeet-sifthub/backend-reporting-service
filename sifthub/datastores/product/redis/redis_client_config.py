from sifthub.configs import redis_configs
from sifthub.datastores.product.redis.redis_client import RedisClient
from sifthub.datastores.product.redis.store import RedisSerializerStore
from sifthub.serializer.msg_pack_serializer import MsgPackSerializer
from sifthub.utils.logger import setup_logger

logger = setup_logger(__name__)

# Initialize Redis client and store
_redis_client = RedisClient(
    host=redis_configs.PRIMARY_REDIS_DATASOURCE_URL,
    port=int(redis_configs.PRIMARY_REDIS_DATASOURCE_PORT),
    password=redis_configs.PRIMARY_REDIS_DATASOURCE_PASSWORD
)

_serializer = MsgPackSerializer()
_redis_store = RedisSerializerStore(_serializer, _redis_client.get_client())

logger.info("INIT Redis Client")


async def get(key: str, hash_key: str):
    """Get value from Redis by key and hash_key"""
    return await _redis_store.get(key, hash_key)


async def set(key: str, hash_key: str, value):
    """Set value in Redis by key and hash_key"""
    return await _redis_store.set(key, hash_key, value)


async def set_ex(key: str, expiry: int, value):
    """Set value in Redis with expiry"""
    return await _redis_store.set_ex(key, expiry, value)


async def get_by_key(key: str):
    """Get value from Redis by key"""
    return await _redis_store.get_by_key(key)


async def remove_by_key(key: str):
    """Remove value from Redis by key"""
    return await _redis_store.remove_by_key(key) 