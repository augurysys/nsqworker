nsqworker - an async task worker for NSQ
----------------------------------------

This is a fork of [NSQWorker](https://github.com/bsphere/nsqworker).
It implements a generic message router.
See [Examples](https://github.com/rikonor/nsqworker/blob/master/nsqworker/example.py).

Currently only a threaded worker is supported.
it handles [NSQ](http://nsq.io) messaging with the official Python/[Tornado](http://tornadoweb.org) library and executes a blocking message handler function in an executor thread pool.

The motivation behind this package is to replace [Celery](http://celeryproject.org)/RabbitMQ worker processes which perform long running tasks with [NSQ](http://nsq.io).


Installation
------------
`pip install git+https://github.com/rikonor/nsqworker.git`

Usage 1
-----
```
import nsq
from nsqhandler import NSQHandler, load_routes, route

@load_routes
class Test(NSQHandler):
    @route(lambda msg: True)
    def test(self, message):
        self.logger.info("Test")

Test("test4", "test")

nsq.run()
```

Usage 2
-----
```
import nsq
import time
from nsqworker import nsqworker


def process_message(message):
  print "start", message.id
  time.sleep(2)
  print "end", message.id

def handle_exc(message, e):
  traceback.print_exc()
  w.io_loop.add_callback(message.requeue)

w = nsqworker.ThreadWorker(message_handler=process_message,
                           exception_handler=handle_exc, concurrency=5, ...)

w.subscribe_reader()

nsq.run()
```

The arguments for the `ThreadWorker` constructor are a synchronous, blocking function that handles messages, concurrency, an optional exception_handler and all other arguments for the official [NSQ](http://nsq.io) Python library - [pynsq](https://pynsq.readthedocs.org).

* The worker will explicitly call `message.finish()` in case the handler function didn't call `message.finish()` or `message.requeue()`.

* The worker will periodically call `message.touch()` every 30s for long running tasks so they won't timeout by nsqd.

* The exception handler is called with a message and an exception as the arguments in case it was given during the worker's initialization and an exception is raised while processing a message.

* Multiple readers can be added for handing messages from multiple topics and channels.

* Any interactions with NSQ from within a thread worker, such as `message.requeue()`, `message.finish()` and publishing message __must be added as callback to the ioloop__

* An optional `timeout=<seconds>` can be added to the worker constructor, if it is defined, after the defined timeout the optional exception handler will invoked with an `nsqworker.errors.TimeoutError`.
Due to `concurrent.futures.ThreadPoolExecutor` limitations it is impossible to cancel the running executor thread and it may continue running even after the timeout exception was raised.

* An optional `--loglevel` command line argument can be provided to set the logging level, default is `INFO`.
The same logging level can be used with other loggers by getting it form the worker with `numric_level = worker.logger.level`

* TODO - message de-duping.


Metrics
-------

There are 3 supported metric types:
* MESSAGES_SENT_COUNTER - nsq_messages_sent_count
* MESSAGES_RETRY_COUNTER - nsq_messages_retry_count
* RECEIVED_MESSAGES_HISTOGRAM - nsq_received_messages histogram

To plug metrics exposing to your app you can either use:
* ```start_metrics_server(port)``` - stars a simple http server on specified port
* ```make_wsgi_app``` - returns a simple wsgi app which you can plug to your already running webserver (best practice is to add it to ```/metrics```)
* if prometheus is already served, importing the nsq project will register the metrics to prometheus' ```REGISTRY```