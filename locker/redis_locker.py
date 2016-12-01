import os

import redis as redis_client
import time

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
        return RedisLock(key, lock_options, self.service_name, self.redis)


class RedisLock:
    """
    This class represent a redis lock object. A client who would like to lock a resource, should create this object
    and hold it. There are 2 main methods: lock, unlock
    """

    def __init__(self, key, lock_options, service_name, redis):
        self.__lock_obj = redis.lock(name=self.get_key(service_name, key), timeout=lock_options.ttl, blocking_timeout=lock_options.timeout,
                                     sleep=LOCKED_RETRY_DURATION)
        self.__retries = lock_options.retries

    def lock(self):
        """
        This method tries to acquire a lock. If a redis error is raised during the process, it will try again for
        ``self.__lock_obj.retries`` times
        :return: True if the resource is locked, False o.w
        :raise: ``RedisError`` in case of redis returned error while trying to lock and all retries were used
        """
        err = None
        for i in xrange(0, self.__retries):
            try:
                return self.__lock_obj.acquire()
            except redis_client.RedisError as re:
                err = re
                if i != self.__lock_obj.retries - 1:
                    time.sleep(ERR_RETRY_DURATION)
                continue
        raise err

    def unlock(self):
        """
        unlocks the lock object
        :return: there is no return for this function, if everything goes well, no exception will be thrown
        :raise: ``RedisError`` - general error with redis, there are 2 special cases where there will be raised
        ``LockError(RedisError)`` if the lock does not exist or if the lock is owned by a different owner (the lock
        token is different).
        """
        self.__lock_obj.release()

    @staticmethod
    def get_key(service_name, resource):
        resource = "".join(resource.split())  # remove all white spaces
        return "{}:lock:{}".format(service_name, resource)


class LockOptions:
    """
    Lock Object, should be used by locking mechanism clients for defining the lock object
    """

    def __init__(self, ttl=DEFAULT_TTL, timeout=DEFAULT_TIMEOUT, retries=DEFAULT_RETRIES):
        """
        Create a new lock object
        ``ttl`` - the expiry time of the lock
        ``timeout`` - the amount of time which the locker will try to get a lock while the resource is locked by
                    another process
        ``retries`` - the amount of retries which in case of redis error the locker will try to lock
        """
        self.ttl = ttl
        self.timeout = timeout
        self.retries = retries
