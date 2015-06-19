import os
import sys
import functools
import traceback
import logging
import json
import re

from tornado import ioloop
import nsq
from nsq import Error
from nsqworker import ThreadWorker

from mdict import *

# Fetch NSQD addres
NSQD_TCP_ADDRESSES = os.environ.get('NSQD_TCP_ADDRESSES', "").split(",")
NSQD_TCP_ADDRESSES.remove("")
LOOKUPD_HTTP_ADDRESSES = os.environ.get('LOOKUPD_HTTP_ADDRESSES', "").split(",")
LOOKUPD_HTTP_ADDRESSES.remove("")

# Temporary
NSQD_TCP_ADDRESSES = ["localhost:4150"]
LOOKUPD_HTTP_ADDRESSES = ["localhost:4161"]

kwargs = {}

if LOOKUPD_HTTP_ADDRESSES:
    kwargs['lookupd_http_addresses'] = LOOKUPD_HTTP_ADDRESSES
elif NSQD_TCP_ADDRESSES:
    kwargs['nsqd_tcp_addresses'] = NSQD_TCP_ADDRESSES
else:
    raise EnvironmentError("Please set NSQD_TCP_ADDRESSES / LOOKUPD_HTTP_ADDRESSES.")


class NSQHandler(object):
    def __init__(self, topic, channel, timeout=None, concurrency=1):
        '''
        :type writer: nsq.Writer

        This is a wrapper around nsqworker.ThreadWorker
        '''
        self.logger = self.__class__.get_logger()
        self.writer = self.__class__.get_writer()
        self.io_loop = ioloop.IOLoop.instance()

        ThreadWorker(
            message_handler=self.handle_message,
            exception_handler=self.handle_exception,
            timeout=timeout,
            concurrency=concurrency,
            topic=topic, channel=channel, **kwargs
        ).subscribe_worker()

        self.routes = []

    @staticmethod
    def get_writer():
        if NSQD_TCP_ADDRESSES:
            return nsq.Writer(nsqd_tcp_addresses=NSQD_TCP_ADDRESSES)

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

    def send_message(self, topic, message):
        ''' A wrapper around io_loop.add_callback and writer.pub for sending a message

        :type topic: str
        :type message: str
        '''
        if self.writer is None:
            raise RuntimeError("Please provide an nsq.Writer object in order to send messages.")

        callback = functools.partial(self.finish_pub, topic=topic, payload=message)
        self.io_loop.add_callback(self.writer.pub, topic, message, callback)

    def send_messages(self, topic, messages):
        ''' A wrapper around io_loop.add_callback and writer.mpub for sending multiple messages at once

        :type topic: str
        :type messages: list[str]
        '''
        if self.writer is None:
            raise RuntimeError("Please provide an nsq.Writer object in order to send messages.")

        callback = functools.partial(self.finish_pub, topic=topic, payload=messages)
        self.io_loop.add_callback(self.writer.mpub, topic, messages, callback)

    def finish_pub(self, conn, data, topic, payload):
        '''
        This method should serve as a callback to the publish/multi-publish method
        It should parse the arguments to decide if the publish was successful or not
        If the publish was not successful, after a pre-defined sleep period, try and resend the message/multi-message
        '''
        delay = 1

        # Parse conn and data to decide whether message failed or not
        if isinstance(data, Error) or conn is None or data != 'OK':
            # Message failed, re-send
            self.logger.error("Message failed, waiting {} seconds before trying again..".format(delay))
            # Take a short break and then try to resend the message
            if isinstance(payload, str):
                self.io_loop.call_later(delay, self.send_message, topic, payload)
            elif isinstance(payload, list):
                self.io_loop.call_later(delay, self.send_messages, topic, payload)
        else:
            self.logger.debug("Sent message {}.".format(payload))

    def register_route(self, route, handler_func):
        ''' Basic route register
        '''
        # Notice: conflicting routes are intentionally allowed
        # so multiple handler functions can be invoked for a single route

        # TODO - add priority to a route handler, then sort by priority in route_message before executing handlers

        self.routes.append((route, handler_func))

    def route_message(self, message, route_field="name"):
        ''' Basic message router

        The basic functionality assumes that a message is json encoded
        that the field to route for is "name"
        and that the route finding method is regex matching

        Handlers for the same route will be run sequentially
        '''
        try:
            message = json.loads(message)
        except ValueError:
            self.logger.error("Non-JSON message received.")
            return

        # Enhance message object with mget method
        class EnhancedMessage(dict):
            get = mget
        message = EnhancedMessage(message)

        handlers = []
        for route, handler in self.routes:
            if re.match(route, message.get(route_field, "")):
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
        ''' Basic message handler
        '''
        self.logger.debug("Received message: {}".format(message.body))
        self.route_message(message.body)
        self.logger.debug("Finished handling message: {}".format(message.body))

    def handle_exception(self, message, e):
        ''' Basic error handler
        '''
        error = "message {} raised an exception: {}. Message body: {}".format(message.id, e, message.body)
        self.logger.error(error)
        self.logger.error(traceback.format_exc())
