import os

import redis as redis_client

REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = os.environ.get("REDIS_PORT")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", None)
if not all([REDIS_HOST, REDIS_PORT]):
    raise EnvironmentError("Please set REDIS_HOST and REDIS_PORT")

DEFAULT_TTL = 10
DEFAULT_TIMEOUT = 10
DEFAULT_RETRIES = 3
LOCKED_RETRY_DURATION = 20
ERR_RETRY_DURATION = 50


class RedisLocker:
    def __init__(self, service_name):
        self.redis = redis_client.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, password=REDIS_PASSWORD)
        self.service_name = service_name

    def get_lock_object(self, key, lock_options):
        return RedisLock(key, lock_options, self.redis)


class RedisLock:
    def __init__(self, key, lock_options, redis):
        self.__lock_obj = redis.lock(name=key, timeout=lock_options.ttl, blocking_timeout=lock_options.timeout,
                                     sleep=LOCKED_RETRY_DURATION)

    def lock(self):
        err = None
        for i in xrange(0,self.__lock_obj.retries):
            try:
                return self.__lock_obj.acquire()
            except redis_client.RedisError as re:
                err = re
                continue
        raise err


    def unlock(self):
        return self.__lock_obj.release()


class LockOptions:
    def __init__(self):
        self.ttl = DEFAULT_TTL
        self.timeout = DEFAULT_TIMEOUT
        self.retries = DEFAULT_RETRIES
