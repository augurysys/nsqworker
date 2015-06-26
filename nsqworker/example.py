from nsqhandler import NSQHandler, load_routes, route
from basic_matchers import json_matcher, regex_matcher
import json
import time


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
