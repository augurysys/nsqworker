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


class NSQHandler(NSQWriter):
    def __init__(self, topic, channel, timeout=None, concurrency=1):
        """
        :type writer: nsq.Writer

        This is a wrapper around nsqworker.ThreadWorker
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

        self.routes = []

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

    def register_route(self, matcher_func, handler_func):
        """Basic route register

        :type matcher_func: function
        :type handler_func: function
        """
        # Notice: conflicting routes are intentionally allowed
        # so multiple handler functions can be invoked for a single route
        self.routes.append((matcher_func, handler_func))

    def route_message(self, message):
        """Basic message router

        Handlers for the same route will be run sequentially
        """
        handlers = []
        for route_matcher, handler in self.routes:
            if route_matcher(message) is True:
                handlers.append(handler)

        if len(handlers) == 0:
            self.logger.warning("No handlers found for message {}.".format(message))
            return

        for handler in handlers:
            self.logger.debug("Routing message to handler {}".format(handler.__name__))
            try:
                handler(message)
            except Exception as e:
                self.logger.error("Handler {} failed handling message {} with error {}".format(
                    handler.__name__, message, e.message))

    def handle_message(self, message):
        """Basic message handler
        """
        self.logger.debug("Received message: {}".format(message.body))
        self.route_message(message.body)
        self.logger.debug("Finished handling message: {}".format(message.body))

    def handle_exception(self, message, e):
        """Basic error handler
        """
        error = "message {} raised an exception: {}. Message body: {}".format(message.id, e, message.body)
        self.logger.error(error)
        self.logger.error(traceback.format_exc())
