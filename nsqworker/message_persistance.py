import json
import os
import time
from datetime import datetime

import redis

MESSAGE_STORE_KEY = "eh:messages:failed"


class MessagePersistor(object):

    def __init__(self, logger):

        self._logger = logger
        self._init_redis()

        if not self._redis:

            self._logger.info("Redis client unavailable, failed message persistence disabled")

    def _init_redis(self):

        self._redis = None

        _host = os.environ.get("REDIS_HOST")
        _port = os.environ.get("REDIS_PORT")
        _password = os.environ.get("REDIS_PASSWORD", None)

        if all([_host, _port]):
            self._redis = redis.StrictRedis(host=_host, port=_port,
                                            db=0, password=_password)

    def persist_message(self, topic, channel, route, message):

        if not self._redis:
            return

        persist_time = datetime.now()

        doc = {
            "topic": topic,
            "channel": channel,
            "route": route,
            "message": message,
            "persisted_at": persist_time.isoformat()
        }

        ts = time.mktime(persist_time.timetuple())

        new = self._redis.zadd(MESSAGE_STORE_KEY, ts, json.dumps(doc))
        return new

    def is_persisted_message(self, message):

        return True if 'recipients' in message else False

    def _is_route_persisted(self, message, channel, route):

        return True if route in message['recipients'].get(channel, []) else False

    def is_route_message(self, message, channel, route):

        if not self.is_persisted_message(message):
            return True

        return self._is_route_persisted(message, channel, route)





