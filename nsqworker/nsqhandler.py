import json
import logging
import os
import random
import string
import sys
import traceback
from string import hexdigits
from functools import wraps
import time

import nsq
from auguryapi.metrics import measure_nsq_latency, measure_nsq_stats
from tornado import ioloop

from nsqworker import ThreadWorker
from helpers import register_nsq_topics_from_env
from nsqwriter import NSQWriter
import locker.redis_locker as _locker
from redis import exceptions as redis_errors

from message_persistance import MessagePersistor

# Fetch NSQD address
NSQD_TCP_ADDRESSES = os.environ.get('NSQD_TCP_ADDRESSES', "").split(",")
if "" in NSQD_TCP_ADDRESSES:
    NSQD_TCP_ADDRESSES.remove("")
LOOKUPD_HTTP_ADDRESSES = os.environ.get('LOOKUPD_HTTP_ADDRESSES', "").split(",")
if "" in LOOKUPD_HTTP_ADDRESSES:
    LOOKUPD_HTTP_ADDRESSES.remove("")
# retry count limit of handling idempotent message
RETRY_LIMIT = os.environ.get('RETRY_LIMIT', '3')
if not RETRY_LIMIT.isdigit():
    raise EnvironmentError("Please set a number to the retry count")
RETRY_LIMIT = int(RETRY_LIMIT)

kwargs = {}

if LOOKUPD_HTTP_ADDRESSES:
    kwargs['lookupd_http_addresses'] = LOOKUPD_HTTP_ADDRESSES
elif NSQD_TCP_ADDRESSES:
    kwargs['nsqd_tcp_addresses'] = NSQD_TCP_ADDRESSES
else:
    raise EnvironmentError("Please set NSQD_TCP_ADDRESSES / LOOKUPD_HTTP_ADDRESSES.")

current_milli_time = lambda: int(round(time.time() * 1000))

def get_random_string():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(7))

def load_routes(cls):
    """Class decorator for NSQHandler subclasses to load all routes

    :type cls: NSQHandler
    """
    funcs = [(member.options, member) for name, member in cls.__dict__.items() if
             getattr(member, 'options', None) is not None]
    for options, handler in funcs:
        for matcher, lock_options, is_idempotent in options:
            # check if lock exist, and wrap handler with lock accordingly
            cls.register_route(matcher, handler, is_idempotent) if lock_options is None else cls.register_route(
                matcher, with_lock(handler, lock_options), is_idempotent)

    return cls


def route(matcher_func, nsq_lock_options=None, is_idempotent=False):
    """Decorator for registering a class method along with it's route (matcher based)
    """

    def wrapper(handler_func):
        if getattr(handler_func, 'options', None) is None:
            handler_func.options = []
        handler_func.options.insert(0, (matcher_func, nsq_lock_options, is_idempotent))
        return handler_func

    return wrapper


def gen_random_string(n=10):
    return ''.join(random.choice(hexdigits) for _ in range(n))


_identity = lambda x: x


def with_lock(handler_func, nsq_lock_options):
    @wraps(handler_func)
    def flock(self, message):
        event = self.extract(message.body)
        event_name = event.get("name")
        resource_id = event.get(nsq_lock_options.path_to_id)
        if resource_id is None:
            self.logger.warning(
                "Cannot find lock resource id on event data path:{}".format(nsq_lock_options.path_to_id))
            if nsq_lock_options.is_mandatory:
                raise ValueError("Mandatory lock acquiring aborted due to lack of resource id on event data")
            else:
                return handler_func(self, message)

        key = "{}:{}".format(event_name, resource_id)
        lock_object = self.locker.get_lock_object(key, nsq_lock_options)

        # locking
        try:
            is_locked = lock_object.lock()
        except redis_errors.RedisError as re:
            self.logger.warning("Acquiring lock failed with error:{}".format(re))
            if nsq_lock_options.is_mandatory:
                raise re
            return handler_func(self, message)
        if is_locked:
            try:
                return handler_func(self, message)
            finally:
                lock_object.unlock()

        # lock not acquired, resource is locked
        self.logger.warning("Acquiring lock timed out - resource {} is locked by another process".format(key))
        if nsq_lock_options.is_mandatory:
            self.logger.error("Lock is mandatory, aborting handler")
            raise _locker.LockerError("Mandatory lock not acquired, aborting handler")

        # lock is not mandatory run handler without lock
        return handler_func(self, message)

    return flock


class NSQHandler(NSQWriter):
    def __init__(self, topic, channel, timeout=None, concurrency=1, max_in_flight=1,
                 message_preprocessor=None, service_name=get_random_string(), raven_client=None):

        """Wrapper around nsqworker.ThreadWorker
        """
        super(NSQHandler, self).__init__()
        self.logger = self.__class__.get_logger()
        self.io_loop = ioloop.IOLoop.instance()
        self.topic = topic
        self.channel = channel
        self.locker = _locker.RedisLocker(service_name, self.logger)
        self.raven_client = raven_client
        self._message_preprocessor = message_preprocessor if message_preprocessor else _identity

        self._persistor = MessagePersistor(self.logger)
        register_nsq_topics_from_env([topic])
        ThreadWorker(
            message_handler=self.handle_message,
            exception_handler=self.handle_exception,
            timeout=timeout,
            concurrency=concurrency,
            max_in_flight=max_in_flight,
            topic=topic, channel=channel, **kwargs
        ).subscribe_worker()

        # self.routes = []

    @classmethod
    def get_logger(cls, name=None):
        logger = logging.getLogger(name or cls.__name__)
        if not logger.handlers:
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

            handler = logging.StreamHandler(stream=sys.stdout)
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO)

            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            logger.propagate = 0

        return logger

    @classmethod
    def register_route(cls, matcher_func, handler_func, is_idempotent):
        """Register route
        """
        if getattr(cls, "routes", None) is None:
            cls.routes = []

        # Don't use bound methods - convert to bare function
        if getattr(handler_func, "im_self", None) is not None:
            handler_func = handler_func.__func__

        cls.routes.append((matcher_func, handler_func, is_idempotent))

    def route_message(self, message):
        """Basic message router

        Handlers for the same route will be run sequentially

        type message: nsq.Message
        """
        m_body = message.body
        handlers = []

        for matcher_func, handler_func, is_idempotent in self.__class__.routes:

            if matcher_func(m_body) is True:
                handlers.append((handler_func, is_idempotent))

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

        route_id = gen_random_string()

        for handler, is_idempotent in handlers:
            status = "OK"

            if jsn is not None and self._persistor.is_persisted_message(jsn):
                if self._persistor.is_route_message(jsn, self.channel, handler.__name__):

                    self.logger.info("[{}] Route {} in channel {} will handle persisted message".format(
                        route_id, self.topic, self.channel, handler.__name__, self.channel
                    ))

                else:
                    continue

            self.logger.info("[{}] [START] [topic={}] [channel={}] [event={}] [route={}] [try_num={}]".format(
                route_id, self.topic, self.channel, event_name, handler.__name__, message.attempts))
            start_time = current_milli_time()
            try:
                handler(self, self._message_preprocessor(message))

            except Exception as e:
                # In case of failure and route is idempotent re-queue the message until retry limit is reached
                if is_idempotent and message.attempts <= RETRY_LIMIT:
                    self.logger.info(
                        "[{}] trying to re-queue failed message, current attempts: [{}] ".format(route_id,
                                                                                                 message.attempts))
                    message.requeue(backoff=True, delay=-1)
                    self.logger.info(
                        "[{}] message re-queued successfully".format(route_id))
                    continue

                status = "FAILED"
                msg = "[{}] Handler {} failed handling message {} with error {}".format(
                    route_id, handler.__name__, message.body, e.message)

                self.logger.error(msg)
                self.handle_exception(message, e, tags={"route": handler.__name__, "error": "new NSQ failed event"})
                if self._persistor.enabled:
                    new = self._persistor.persist_message(self.topic, self.channel, handler.__name__, m_body,
                                                          repr(e))
                    if new:
                        self.logger.info("[{}] Persisted failed message".format(route_id))
                    else:
                        self.logger.info("[{}] Updated existing failed message".format(route_id))

            measure_nsq_latency(duration=float(current_milli_time() - start_time),
                                topic=self.topic, channel=self.channel,
                                event=event_name, route=handler.__name__)
            measure_nsq_stats(status=status, topic=self.topic, channel=self.channel,
                              event=event_name, route=handler.__name__)

            self.logger.info(
                "[{}] [END] [topic={}] [channel={}] [event={}] [route={}] [try_num={}] [status={}] [time={}]".format(
                    route_id, self.topic, self.channel, event_name, handler.__name__, message.attempts, status,
                    str(current_milli_time() - start_time)))

    def handle_message(self, message):
        """
        Basic message handler
        :type message: nsq.Message
        """

        self.logger.debug("Received message: {}".format(message.body))
        self.route_message(message)
        self.logger.debug("Finished handling message: {}".format(message.body))

    def handle_exception(self, message, e, notify=True, tags=None):
        """
        Basic error handler
        :type message: nsq.Message
        :type e: Exception
        :type tags: dict
        :type notify: bool
        """
        error = "message raised an exception: {}. Message body: {}".format(e, message.body)
        self.logger.error(error)
        self.logger.error(traceback.format_exc())
        if notify and self.raven_client is not None:
            self.raven_client.captureException(message=message.body, error_message=e.message, tags=tags)

