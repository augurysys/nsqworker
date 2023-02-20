import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor

import nsq
from tornado import gen
from tornado import ioloop
from tornado.concurrent import run_on_executor

try:
    from errors import TimeoutError
except ModuleNotFoundError:
    from nsqworker.errors import TimeoutError


class ThreadWorker:
    def __init__(self, message_handler=None, exception_handler=None,
                 concurrency=1, max_in_flight=1, timeout=None, service_name="no_name", **kwargs):
        self.io_loop = ioloop.IOLoop.instance()
        self.executor = ThreadPoolExecutor(concurrency)
        self.concurrency = concurrency
        self.max_in_flight = max_in_flight
        self.kwargs = kwargs
        self.message_handler = message_handler
        self.exception_handler = exception_handler
        self.timeout = timeout
        self.service_name = service_name

        self.logger = ThreadWorker.get_logger()

    @staticmethod
    def get_logger():
        logger = logging.getLogger("ThreadWorker")
        if not logger.handlers:
            parser = argparse.ArgumentParser()
            parser.add_argument('--loglevel', nargs='?', help='log level', default='INFO')
            args = parser.parse_args()

            level = getattr(logging, args.loglevel.upper())
            if not isinstance(level, int):
                raise ValueError('Invalid log level: %s' % args.loglevel)

            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

            handler = logging.StreamHandler(stream=sys.stdout)
            handler.setFormatter(formatter)
            handler.setLevel(level)

            logger.addHandler(handler)
            logger.setLevel(level)
            logger.propagate = 0

        return logger

    @run_on_executor
    def _run_threaded_handler(self, message):
        self.message_handler(message)

    @gen.coroutine
    def _message_handler(self, message):
        """
        :type message: nsq.Message
        """
        if isinstance(message.body, bytes):
            message.body = str(message.body, 'utf-8')
        self.logger.debug("Received message %s", message.id)
        message.enable_async()

        def touch():
            self.logger.debug("Sending touch event for message %s", message.id)
            try:
                message.touch()
            except AssertionError:
                self.logger.debug("touch() raised an exception - ignore it")

        p = ioloop.PeriodicCallback(touch, 30000)
        p.start()

        def timeout_handler():
            p.stop()

            error = \
                "Message handler {} in {} for message {} exceeded timeout: {}".format(self.message_handler,
                                                                                      self.message_handler.__module__,
                                                                                      message.id, message.body)

            self.logger.error(error)
            if self.exception_handler is not None:
                self.exception_handler(message, TimeoutError(error))

        timeout = None
        if self.timeout is not None:
            timeout = self.io_loop.call_later(self.timeout, timeout_handler)

        try:
            result = self._run_threaded_handler(message)
            yield result
            result.exception()
        except Exception as e:
            self.logger.debug("Message handler for message %s raised an exception", message.id)
            if self.exception_handler is not None:
                self.exception_handler(message, e)

        p.stop()

        if timeout is not None:
            self.io_loop.remove_timeout(timeout)

        if not message.has_responded():
            message.finish()

        self.logger.debug("Finished handling message %s", message.id)

    def subscribe_worker(self):
        kwargs = {k: v for k, v in self.kwargs.items()}

        kwargs["message_handler"] = self._message_handler
        kwargs["max_in_flight"] = self.max_in_flight

        self.reader = nsq.Reader(**kwargs)

        self.logger.info("Added an handler for NSQD messages on [service_name={}] [topic={}], [channel={}].".format(
            self.service_name, self.kwargs["topic"], self.kwargs["channel"]))
        self.logger.info("Handling messages with {} threads and {} max_in_flight.".format(
            self.concurrency, self.max_in_flight))
