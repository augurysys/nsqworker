from nsqhandler import NSQHandler
import json
import time

import re

from mdict import MDict

def inject_mget(message):
    return MDict(message)

def json_matcher(field, value):
    def match(message):
        try:
            message = json.loads(message)
        except ValueError:
            return False

        return message.get(field) == value

    return match

def regex_matcher(pattern):
    def match(message):
        return re.match(pattern, message) is not None

    return match


class JSONPingPong(NSQHandler):
    def __init__(self, topic, channel):
        super(JSONPingPong, self).__init__(topic, channel)

        self.register_route(json_matcher("name", "request.ping"), self.ping)
        self.register_route(json_matcher("name", "request.pong"), self.pong)

    def ping(self, message):
        self.logger.info("Ping")
        time.sleep(1)
        self.send_message("test", json.dumps(dict(name="request.pong")))

    def pong(self, message):
        self.logger.info("Pong")
        time.sleep(1)
        self.send_message("test", json.dumps(dict(name="request.ping")))

class TextPingPong(NSQHandler):
    def __init__(self, topic, channel):
        super(TextPingPong, self).__init__(topic, channel)

        self.register_route(regex_matcher("^ping$"), self.ping)
        self.register_route(regex_matcher("^pong$"), self.pong)

    def ping(self, message):
        self.logger.info("Ping")
        time.sleep(1)
        self.send_message("test", "pong")

    def pong(self, message):
        self.logger.info("Pong")
        time.sleep(1)
        self.send_message("test", "ping")

import nsq

# JSONPingPong("test", "pingpong")
TextPingPong("test", "pingpong")

nsq.run()


# TODO - Create decorator for register_route to put right above handler functions
# TODO - Add priorities
# TODO - loglevel setting of nsqhandler (to be like ThreadWorker)
# TODO - message encryption
