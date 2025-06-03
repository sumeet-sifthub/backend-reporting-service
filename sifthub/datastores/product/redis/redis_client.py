from redis import asyncio as aioredis


class RedisClient(object):

    def __init__(self,
                 host: str,
                 port: int,
                 password: str | None,
                 **kwargs):
        if password:
            self.redis = aioredis.Redis(
                host=host,
                port=port,
                password=password,
                ssl=True,
                ssl_cert_reqs='none',
                **kwargs,
            )
            return
        self.redis = aioredis.Redis(
            host=host,
            port=port,
            **kwargs,
        )

    def get_client(self):
        return self.redis