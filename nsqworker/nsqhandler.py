import json
import logging
import os
import random
import sys
import traceback
from string import hexdigits

import nsq
from tornado import ioloop

from nsqworker import ThreadWorker
from nsqwriter import NSQWriter

from message_persistance import MessagePersistor

# Fetch NSQD address
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
    funcs = [(member.matcher_funcs, member) for name, member in cls.__dict__.items() if
             getattr(member, 'matcher_funcs', None) is not None]
    for matchers, handler in funcs:
        for matcher in matchers:
            cls.register_route(matcher, handler)

    return cls


def route(matcher_func, lock_dict=None):
    """Decorator for registering a class method along with it's route (matcher based)
    """

    def wrapper(handler_func):
        if getattr(handler_func, 'matcher_funcs', None) is None:
            handler_func.matcher_funcs = []
        handler_func.matcher_funcs.insert(0, matcher_func)


        if lock_dict is None:
            return handler_func
        else:
            def flock(self, message):
                logging.warning("No lock needed!")
                print "Locking"
                try:
                    handler_func(self, message)
                except Exception as e:
                    print "got exception, unlocking"
                    raise e
                    return

                print "Un-Locking"
            flock.matcher_funcs = handler_func.matcher_funcs
            return flock

    return wrapper


def gen_random_string(n=10):

    return ''.join(random.choice(hexdigits) for _ in range(n))

_identity = lambda x: x

class NSQHandler(NSQWriter):
    def __init__(self, topic, channel, timeout=None, concurrency=1,
                 message_preprocessor=None):

        """Wrapper around nsqworker.ThreadWorker
        """
        super(NSQHandler, self).__init__()
        self.logger = self.__class__.get_logger()
        self.io_loop = ioloop.IOLoop.instance()
        self.topic = topic
        self.channel = channel

        self._message_preprocessor = message_preprocessor if message_preprocessor else _identity

        self._persistor = MessagePersistor(self.logger)

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

        type message: nsq.Message
        """
        m_body = message.body
        handlers = []

        for matcher_func, handler_func in self.__class__.routes:
            if matcher_func(m_body) is True:
                handlers.append(handler_func)

        if len(handlers) == 0:
            self.logger.debug("No handlers found for message {}.".format(message.body))
            return

        event_name = "<undefined>"
        jsn = None
        try:
            jsn = json.loads(m_body)
            event_name = jsn['name']
        except Exception:
            pass

        handler_id = gen_random_string()

        self.logger.info("[START] [{}] [{}] [{}] [{}]".format(
            handler_id, self.topic, self.channel, event_name
        ))

        for handler in handlers:
            route_id = gen_random_string()

            if jsn is not None and self._persistor.is_persisted_message(jsn):
                if self._persistor.is_route_message(jsn, self.channel, handler.__name__):

                    self.logger.info("[{}] Route {} in channel {} will handle persisted message".format(
                        route_id, handler.__name__, self.channel
                    ))

                else:
                    continue

            self.logger.info("[{}] Routing message to handler {}".format(
                route_id, handler.__name__)
            )

            try:

                handler(self, self._message_preprocessor(message))

            except Exception as e:
                msg = "[{}] Handler {} failed handling message {} with error {}".format(
                    route_id, handler.__name__, message.body, e.message)
                self.logger.error(msg)
                self.handle_exception(message, e)

                if self._persistor.enabled:
                    new = self._persistor.persist_message(self.topic, self.channel, handler.__name__, m_body)
                    if new:
                        self.logger.info("[{}] Persisted failed message".format(route_id))
                    else:
                        self.logger.info("[{}] Updated existing failed message".format(route_id))

            self.logger.info("[{}] Done handling {}".format(
                route_id, handler.__name__)
            )

        self.logger.info("[END] [{}] [{}] [{}] [{}]".format(
            handler_id, self.topic, self.channel, event_name
        ))

    def handle_message(self, message):
        """
        Basic message handler
        :type message: nsq.Message
        """

        self.logger.debug("Received message: {}".format(message.body))
        self.route_message(message)
        self.logger.debug("Finished handling message: {}".format(message.body))

    def handle_exception(self, message, e):
        """
        Basic error handler
        :type message: nsq.Message
        :type e: Exception
        """
        error = "message raised an exception: {}. Message body: {}".format(e, message.body)
        self.logger.error(error)
        self.logger.error(traceback.format_exc())

