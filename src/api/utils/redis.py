import aioredis

from typing import Optional

# no need to wrap indeed


class RedisUtils(object):
    def __init__(self, redis_url: str,
                 password: Optional[str] = None,
                 decode_responses: Optional[bool] = True,
                 **kwargs):
        self.redis = aioredis.from_url(redis_url,
                                       password=password,
                                       decode_responses=decode_responses,
                                       **kwargs)

    def __getattr__(self, func):
        return getattr(self.redis, func)
