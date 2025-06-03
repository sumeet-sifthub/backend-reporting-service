import logging as logger

from sifthub.configs.http_configs import CLIENT_SERVICE_HOST, USER_ROLE_MAPPING_DATA_LOAD_CACHE_BY_ID_ENDPOINT
from sifthub.datastores.product.redis import redis_client_config
from sifthub.utils import httputil

__USER_ROLE_CONFIG_KEY = "USER_ROLE_ACCESS"


async def __build_hash_key_by_user_id(user_id: int, client_id: int, product_id: int):
    return "CLIENT_" + str(client_id) + "_PRODUCT_" + str(product_id) + "_USERID_" + str(user_id)


async def find_role_mapping_by_user_id(user_id: int, client_id: int, product_id: int):
    hash_key = await __build_hash_key_by_user_id(user_id, client_id, product_id)
    user_role_data = await redis_client_config.get(__USER_ROLE_CONFIG_KEY,
                                                   hash_key)
    if user_role_data is None:
        logger.info("Role Mapping for User Id: " + str(user_id) + " not found in cache ")
        user_role_data = await load_user_role_data_by_user_id(user_id, client_id, product_id)
    return user_role_data


async def load_user_role_data_by_user_id(user_id: int, client_id: int, product_id: int):
    return await httputil.get(CLIENT_SERVICE_HOST, USER_ROLE_MAPPING_DATA_LOAD_CACHE_BY_ID_ENDPOINT +
                              str(user_id) + "/" + str(client_id) + "/" + str(product_id), {}, {})
