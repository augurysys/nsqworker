from nsqhandler import NSQHandler, load_routes, route
from basic_matchers import json_matcher, regex_matcher
import json
import time

# ---------------
# -- Example 1 --
# ---------------
# Basic handler functionality requires subclassing NSQHandler
# Calling load_routes decorator on it
# Calling route(matcher_func, handler_func) on every handler

@load_routes
class JSONPingPong(NSQHandler):
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

# ---------------
# -- Example 2 --
# ---------------
# Notice you can register multiple routes for one handler

@load_routes
class TextPingPong(NSQHandler):
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

# ---------------
# -- Example 3 --
# ---------------
# It's also possible to register routes using the class method register_route
# instead of the route decorator, just remember to call super for NSQHandler

@load_routes
class ThirdPingPong(NSQHandler):
    def __init__(self, topic, channel):
        super(ThirdPingPong, self).__init__(topic, channel)
        self.register_route(regex_matcher("^ping$"), self.ping)
        self.register_route(regex_matcher("^pong$"), self.pong)

    def ping(self, message):
        self.logger.info("Ping")
        time.sleep(1)
        self.send_message("test3", "pong")

    def pong(self, message):
        self.logger.info("Pong")
        time.sleep(1)
        self.send_message("test3", "ping")

import nsq

JSONPingPong("test", "pingpong")
TextPingPong("test2", "pingpong2")
ThirdPingPong("test3", "pingpong3")

nsq.run()


# TODO - take get_logger outside of functions, maybe make it into it's own package
# TODO - Add priorities
# TODO - loglevel setting of nsqhandler (to be like ThreadWorker)
# TODO - message encryption
# TODO - switch env vars to cli arguments
