from nsqhandler import NSQHandler, load_routes, route
import json
import time

import re


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


@load_routes
class DecoratorJSONPingPong(NSQHandler):
    routes = []

    @route(json_matcher("name", "ping"))
    def ping(self, message):
        self.logger.info("Ping")
        time.sleep(1)
        self.send_message("test", json.dumps(dict(name="pong")))

    @route(json_matcher("name", "pong"))
    def pong(self, message):
        self.logger.info("Pong")
        time.sleep(1)
        self.send_message("test", json.dumps(dict(name="ping")))

@load_routes
class DecoratorTextPingPong(NSQHandler):
    routes = []

    @route(regex_matcher("^plop$"))
    @route(regex_matcher("^ping$"))
    def ping(self, message):
        self.logger.info("Ping")
        time.sleep(1)
        self.send_message("test2", "pong")

    @route(regex_matcher("^pong$"))
    def pong(self, message):
        self.logger.info("Pong")
        time.sleep(1)
        self.send_message("test2", "ping")


import nsq

DecoratorJSONPingPong("test", "pingpong")
DecoratorTextPingPong("test2", "pingpong2")

nsq.run()


# TODO - Add priorities
# TODO - loglevel setting of nsqhandler (to be like ThreadWorker)
# TODO - message encryption
# TODO - switch env vars to cli arguments
