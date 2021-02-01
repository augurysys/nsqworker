import nsq
from tornado import ioloop

from nsqworker.nsqworker import get_logger


class ProcessWorker:
    def __init__(self, message_handler=None, exception_handler=None,
                 max_in_flight=1, timeout=None, service_name="no_name", **kwargs):
        self.logger = get_logger(self.__class__.__name__)
        self.io_loop = ioloop.IOLoop.instance()
        self.max_in_flight = max_in_flight
        self.kwargs = kwargs
        self.message_handler = message_handler
        self.exception_handler = exception_handler
        self.timeout = timeout
        self.service_name = service_name
        self.reader = None

    def _message_handler(self, message):
        """
        :type message: nsq.Message
        """
        self.logger.debug("Received message %s", message.id)
        message.enable_async()

        def touch():
            self.logger.debug("Sending touch event for message %s", message.id)
            message.touch()

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
            result = self.message_handler(message)
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
        self.logger.info("Handling messages with a single thread and {} max_in_flight.".format(
            self.max_in_flight))
