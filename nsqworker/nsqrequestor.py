import random
import string
import threading
import tornado.ioloop

from nsqwriter import NSQWriter
from nsqhandler import NSQHandler

import time

class RequestorHandler(NSQHandler):
    def __init__(self, topic, channel):
        super(RequestorHandler, self).__init__(topic, channel)

class NSQRequestor(object):
    """
    Notice: Not thread safe at all.
    """
    def __init__(self):
        self.reset_handler()
        self.reset_writer()

    def reset_handler(self):
        self.handler = None
        self.handler_io_loop = None

    def reset_writer(self):
        self.writer = None
        self.writer_io_loop = None

    def start_handler(self):
        # Listen on a random channel
        # This way the channel is always fresh and empty
        self.topic = "".join(random.sample(string.letters + string.digits, 10))
        self.channel = "".join(random.sample(string.letters + string.digits, 10))

        self.handler_thread = threading.Thread(target=self._start_handler)
        self.handler_thread.start()

        while getattr(self.handler_io_loop, "_running", False) is not True:
            pass

        # Insert artificial delay for writer to stabilize
        time.sleep(0.01)

    def _start_handler(self):
        """
        Call this method when you want the message catcher to run in the background
        """
        self.handler = RequestorHandler(self.topic, self.channel)
        self.handler_io_loop = tornado.ioloop.IOLoop.current()
        self.handler_io_loop.start()

    def stop_handler(self):
        """ Kill the io_loop, otherwise your program can't exit
        """
        self.handler_io_loop.stop()
        self.reset_handler()

        # Insert artificial delay for writer to stabilize
        time.sleep(0.01)

    def wait_for_response(self, timeout=60):
        raise NotImplementedError

    def start_writer(self):
        self.writer_thread = threading.Thread(target=self._start_writer)
        self.writer_thread.start()

        while getattr(self.writer_io_loop, "_running", False) is not True:
            pass

        # Insert artificial delay for writer to stabilize
        time.sleep(0.01)

    def _start_writer(self):
        """
        Call this method when you want the writer to run in the background
        """
        self.writer = NSQWriter()
        self.writer_io_loop = tornado.ioloop.IOLoop.current()
        self.writer_io_loop.start()

    def stop_writer(self):
        """ Kill the io_loop, otherwise your program can't exit
        """
        self.writer_io_loop.stop()
        self.reset_writer()

        # Insert artificial delay for writer to stabilize
        time.sleep(0.01)

    def make_request(self, topic, message, wait_for_response=False):
        if wait_for_response is True:
            self.start_handler()

        self.start_writer()
        self.writer.send_message(topic, message)
        self.stop_writer()

        if wait_for_response is True:
            response = self.wait_for_response(timeout=60)
            self.stop_handler()

            return response


if __name__ == "__main__":
    import json

    requestor = NSQRequestor()

    request = dict(name="request.ping")
    requestor.make_request("test", json.dumps(request))

    # response = requestor.make_request("test", json.dumps(request), wait_for_response=True)

