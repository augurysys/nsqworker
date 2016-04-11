import json
import os
from datetime import datetime

from pymongo import MongoClient


class MessagePersistor(object):

    def __init__(self):

        self._client = self._db = None
        self._init_mongodb()

    def _init_mongodb(self):
        mongodb_url = os.environ.get("MONGODB_URL")
        mongodb_db = os.environ.get("MONGODB_DB")

        if not all((mongodb_url,mongodb_db)):
            print "Mongodb unavailable, message persistence is disabled"
            return

        self._client = MongoClient(mongodb_url)
        self._db = self._client[mongodb_db]

    def persist_message(self, topic, channel, route, message):

        if not self._db:
            return None

        doc = {
            "topic": topic,
            "channel": channel,
            "route": route,
            "message": json.dumps(message, sort_keys=True),
            "persist_time": datetime.now()
        }

        _id = self._db.message_store.save(doc)

        return _id

    def is_persisted_message(self, message):

        return True if 'persisted' in message else False

    def _is_route_persisted(self, message, channel, route):

        return True if route in message['persisted'].get(channel, []) else False

    def is_route_message(self, message, channel, route):

        if not self.is_presisted_message(message):
            return True

        return self._is_route_persisted(message, channel, route)





