from collections import defaultdict
import logging
import json
from datetime import datetime
import time
import sys
import nsq

from utils import gen_random_string

_nonsq = None

def nonsq_enabled():
    return _nonsq is not None

def init_nonsq():
    global _nonsq
    _nonsq = _NoNSQ()

def NoNSQ():
    return _nonsq

def new_message(md, timestamp=None, attempts=0):
    _id = gen_random_string()

    if timestamp is None:
        timestamp = datetime.now()
    
    ts = int(time.mktime(timestamp.timetuple()))

    return nsq.Message(_id, json.dumps(md), ts, attempts)


class _NoNSQ(object):

    def __init__(self):
        self.handlers = {}
        self.sent_messages = defaultdict(list)
        logger = logging.Logger("NoNSQ")

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)

        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = 0

        self._logger = logger
    
    def register_handler(self, handler):
        topic = self.handlers.setdefault(handler.topic, defaultdict(list))
        topic[handler.channel].append({"handler":handler, "queue":[]})

    def send_message(self, topic, message, external=True):
        if not external:
            self.sent_messages[topic].append(message)
        
        result = {}

        channels = self.handlers.get(topic)
        if not channels:
            self._logger.info("no handlers for topic {}".format(topic))
            return

        for channel, handlers in channels.items():
            self._logger.info("sending message on topic {} channel {} ({} handlers)".format(topic, 
            channel, len(handlers)))

            for h in handlers:
                res = h['handler'].route_message(message)
                self._logger.info("handler {}, result {}".format(h.__name__, res))

                h['handler']['queue'].append(res)

                result.setdefault(channel, []).append({'handler':h.__name__, 'result':res})
        
        return result

    def send_messages(self, topic, messages, external=True):
        for m in messages:
            self.send_message(topic, m, external)


    