import os
import sys
import functools
import logging

from tornado import ioloop
import nsq
from nsq import Error


BYTES_MAX_SIZE = os.environ.get('BYTES_MAX_SIZE', '1048576')
if not BYTES_MAX_SIZE.isdigit():
    raise EnvironmentError("Please set a number to the BYTES_MAX_SIZE")
BYTES_MAX_SIZE = int(BYTES_MAX_SIZE)

# Fetch NSQD addres
NSQD_TCP_ADDRESSES = os.environ.get('NSQD_TCP_ADDRESSES', "").split(",")
if "" in NSQD_TCP_ADDRESSES:
    NSQD_TCP_ADDRESSES.remove("")


class NSQWriter(object):
    def __init__(self):
        self.logger = self.__class__.get_logger()
        self.writer = self.get_writer()
        self.io_loop = ioloop.IOLoop.current()

    def get_writer(self):
        if len(NSQD_TCP_ADDRESSES) == 0:
            self.logger.warning("Writer functionality is DISABLED. To enable it please provide NSQD_TCP_ADDRESSES.")
            return None

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

    def send_message(self, topic, message, delay=None):
        """ A wrapper around io_loop.add_callback and writer.pub for sending a message

        :type topic: str
        :type message: str
        :type delay: int
        """
        if self.writer is None:
            raise RuntimeError("Please provide an nsq.Writer object in order to send messages.")

        callback = functools.partial(self.finish_pub, topic=topic, payload=message)

        bytes_size = len(message)
        if bytes_size > BYTES_MAX_SIZE:
            raise ValueError("Message is too big. message={} in topic={}".format(message, topic))

        if delay is not None:
            self.io_loop.add_callback(self.writer.dpub, topic, delay, message, callback)
        else:
            self.io_loop.add_callback(self.writer.pub, topic, message, callback)

    def send_messages(self, topic, messages):
        """ A wrapper around io_loop.add_callback and writer.mpub for sending multiple messages at once

        :type topic: str
        :type messages: list[str]
        """
        if self.writer is None:
            raise RuntimeError("Please provide an nsq.Writer object in order to send messages.")

        callback = functools.partial(self.finish_pub, topic=topic, payload=messages)
        self.io_loop.add_callback(self.writer.mpub, topic, messages, callback)

    def finish_pub(self, conn, data, topic, payload):
        """
        This method should serve as a callback to the publish/multi-publish method
        It should parse the arguments to decide if the publish was successful or not
        If the publish was not successful, after a pre-defined sleep period, try and resend the message/multi-message
        """
        delay = 1

        # Parse conn and data to decide whether message failed or not
        # Parse conn and data to decide whether message failed or not
        if isinstance(data, Error) or conn is None or data != 'OK':
            # Message failed, re-send
            self.logger.error('[connection=%s] failed to PUBLISH [topic=%s], [data=%s]', conn.id if conn else 'NA',
                              topic, data)
            self.logger.error("Message failed, waiting {} seconds before trying again..".format(delay))
            # Take a short break and then try to resend the message
            if isinstance(payload, str):
                self.io_loop.call_later(delay, self.send_message, topic, payload)
            elif isinstance(payload, list):
                self.io_loop.call_later(delay, self.send_messages, topic, payload)
        else:
            self.logger.debug("Sent message {}.".format(payload))
