from sifthub.configs import redis_configs
from sifthub.datastores.product.redis.redis_client import RedisClient
from sifthub.datastores.product.redis.store import RedisSerializerStore
from sifthub.serializer.msg_pack_serializer import MsgPackSerializer
from sifthub.utils.logger import setup_logger

logger = setup_logger()

redis_config = {
    "socket_timeout": redis_configs.REDIS_CONFIGURATION_POOL_CONFIG_TIMEOUT,
    "socket_connect_timeout": redis_configs.REDIS_CONFIGURATION_POOL_CONFIG_TIMEOUT,
    "decode_responses": False,
    "client_name": 'backend-reporting-service-client',
    "max_connections": redis_configs.REDIS_CONFIGURATION_POOL_CONFIG_MAX_ACTIVE
}

logger.info("INIT Redis Client")
redis = RedisClient(host=redis_configs.PRIMARY_REDIS_DATASOURCE_URL,
                    port=redis_configs.PRIMARY_REDIS_DATASOURCE_PORT,
                    password=redis_configs.PRIMARY_REDIS_DATASOURCE_PASSWORD,
                    **redis_config).get_client()
redis_client_config = RedisSerializerStore(MsgPackSerializer(), redis)
