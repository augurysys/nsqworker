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
from tornado import ioloop

from nsqworker import ThreadWorker
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

RETRY_DELAY_DURATION = 1000

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
    def __init__(self, topic, channel, timeout=None, concurrency=1,
                 message_preprocessor=None, service_name=get_random_string()):

        """Wrapper around nsqworker.ThreadWorker
        """
        super(NSQHandler, self).__init__()
        self.logger = self.__class__.get_logger()
        self.io_loop = ioloop.IOLoop.instance()
        self.topic = topic
        self.channel = channel
        self.locker = _locker.RedisLocker(service_name, self.logger)

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

        handler_id = gen_random_string()

        self.logger.info("[{}] [Handling new event] [topic={}] [channel={}] [event={}]".format(
            handler_id, self.topic, self.channel, event_name
        ))

        for handler, is_idempotent in handlers:
            status = "OK"
            route_id = gen_random_string()

            if jsn is not None and self._persistor.is_persisted_message(jsn):
                if self._persistor.is_route_message(jsn, self.channel, handler.__name__):

                    self.logger.info("[{}] Route {} in channel {} will handle persisted message".format(
                        route_id, handler.__name__, self.channel
                    ))

                else:
                    continue

            self.logger.info("[{}] [START] Routing message to handler [route={}] [event={}]".format(
                route_id, handler.__name__, event_name)
            )
            start_time = current_milli_time()
            try:

                handler(self, self._message_preprocessor(message))

            except Exception as e:
                if is_idempotent:
                    retry_count = jsn['retry_count'] + 1 if "retry_count" in jsn else 1
                    if retry_count < RETRY_LIMIT:
                        # in case we did not reach the retry limit, we send a new message with the specific failed
                        # recipient and increasing the retry counter. We cannot use nsq's requeue mechanism because
                        # we must send the event to the specific handler so we wont run all related handlers again
                        # (which are not guaranteed to be idempotent as well)
                        self._construct_recovery_message(jsn, handler.__name__, retry_count)
                        self.send_message(self.topic, json.dumps(jsn), delay=RETRY_DELAY_DURATION * retry_count)
                        self.logger.info("[{}] [END] [route={}] [event={}] [status={}] [retry_count={}] [time={}] "
                                         .format(route_id, handler.__name__, event_name, "RESENT", retry_count,
                                                 str(current_milli_time() - start_time)))
                        continue

                status = "FAILED"
                msg = "[{}] Handler {} failed handling message {} with error {}".format(
                    route_id, handler.__name__, message.body, e.message)

                self.logger.error(msg)

                self.handle_exception(message, e)
                if self._persistor.enabled:
                    new = self._persistor.persist_message(self.topic, self.channel, handler.__name__, m_body,
                                                          repr(e))
                    if new:
                        self.logger.info("[{}] Persisted failed message".format(route_id))
                    else:
                        self.logger.info("[{}] Updated existing failed message".format(route_id))

            self.logger.info("[{}] [END] [route={}] [event={}] [status={}] [time={}] ".format(
                route_id, handler.__name__, event_name, status, str(current_milli_time() - start_time))
            )

        self.logger.info("[{}] [DONE handling new event] [topic={}] [channel={}] [event={}]".format(
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

    def _construct_recovery_message(self, msg_json, handler_name, retry_count):

        msg_json['recipients'] = {
            self.channel: [handler_name]
        }
        msg_json['retry_count'] = retry_count
