import os
import sys
import traceback
import logging

from tornado import ioloop
import nsq
from nsqworker import ThreadWorker
from nsqwriter import NSQWriter

# Fetch NSQD addres
NSQD_TCP_ADDRESSES = os.environ.get('NSQD_TCP_ADDRESSES', "").split(",")
if "" in NSQD_TCP_ADDRESSES:
    NSQD_TCP_ADDRESSES.remove("")
LOOKUPD_HTTP_ADDRESSES = os.environ.get('LOOKUPD_HTTP_ADDRESSES', "").split(",")
if "" in LOOKUPD_HTTP_ADDRESSES:
    LOOKUPD_HTTP_ADDRESSES.remove("")

kwargs = {}

if LOOKUPD_HTTP_ADDRESSES:
    kwargs['lookupd_http_addresses'] = LOOKUPD_HTTP_ADDRESSES
elif NSQD_TCP_ADDRESSES:
    kwargs['nsqd_tcp_addresses'] = NSQD_TCP_ADDRESSES
else:
    raise EnvironmentError("Please set NSQD_TCP_ADDRESSES / LOOKUPD_HTTP_ADDRESSES.")


def load_routes(cls):
    """Class decorator for NSQHandler subclasses to load all routes

    :type cls: NSQHandler
    """
    funcs = [(member.matcher_funcs, member) for name, member in cls.__dict__.items() if getattr(member, 'matcher_funcs', None) is not None]
    for matchers, handler in funcs:
        for matcher in matchers:
            cls.register_route(matcher, handler)

    return cls


def route(matcher_func):
    """Decorator for registering a class method along with it's route (matcher based)
    """
    def wrapper(handler_func):
        if getattr(handler_func, 'matcher_funcs', None) is None:
            handler_func.matcher_funcs = []
        handler_func.matcher_funcs.insert(0, matcher_func)
        return handler_func
    return wrapper


class NSQHandler(NSQWriter):
    def __init__(self, topic, channel, timeout=None, concurrency=1):
        """Wrapper around nsqworker.ThreadWorker
        """
        super(NSQHandler, self).__init__()
        self.logger = self.__class__.get_logger()
        self.io_loop = ioloop.IOLoop.instance()

        ThreadWorker(
            message_handler=self.handle_message,
            exception_handler=self.handle_exception,
            timeout=timeout,
            concurrency=concurrency,
            topic=topic, channel=channel, **kwargs
        ).subscribe_worker()

        # self.routes = []

    @classmethod
    def get_logger(cls, name=None):
        logger = logging.getLogger(name or cls.__name__)
        if not logger.handlers:
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

            handler = logging.StreamHandler(stream=sys.stdout)
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO)

            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            logger.propagate = 0

        return logger

    @classmethod
    def register_route(cls, matcher_func, handler_func):
        """Register route
        """
        if getattr(cls, "routes", None) is None:
            cls.routes = []

        # Don't use bound methods - convert to bare function
        if getattr(handler_func, "im_self", None) is not None:
            handler_func = handler_func.__func__

        cls.routes.append((matcher_func, handler_func))

    def route_message(self, message):
        """Basic message router

        Handlers for the same route will be run sequentially
        """
        handlers = []
        for matcher_func, handler_func in self.__class__.routes:
            if matcher_func(message.body) is True:
                handlers.append(handler_func)

        if len(handlers) == 0:
            self.logger.debug("No handlers found for message {}.".format(message))
            return

        for handler in handlers:
            self.logger.debug("Routing message to handler {}".format(handler.__name__))
            try:
                handler(self, message)
            except Exception as e:
                msg = "Handler {} failed handling message {} with error {}".format(
                    handler.__name__, message, e.message)
                self.logger.error(msg)
                self.handle_exception(message, e)

    def handle_message(self, message):
        """Basic message handler
        """
        self.logger.debug("Received message: {}".format(message.body))
        self.route_message(message)
        self.logger.debug("Finished handling message: {}".format(message.body))

    def handle_exception(self, message, e):
        """Basic error handler
        """
        error = "message raised an exception: {}. Message body: {}".format(e, message)
        self.logger.error(error)
        self.logger.error(traceback.format_exc())
