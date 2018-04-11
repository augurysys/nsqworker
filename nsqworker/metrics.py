import prometheus_client

MESSAGES_SENT_COUNTER = prometheus_client.Counter(
    'nsq_messages_sent_count',
    'number of messages sent to topic',
    ['topic'])
MESSAGES_RETRY_COUNTER = prometheus_client.Counter(
    'nsq_messages_retry_count',
    'number of messages retry by topic',
    ['topic'])
RECEIVED_MESSAGES_HISTOGRAM = prometheus_client.Histogram(
    'nsq_received_messages',
    'histogram of received messages',
    ['topic', 'channel', 'status', 'event'])


def start_metrics_server(port):
    prometheus_client.start_http_server(port)


def make_wsgi_app():
    return prometheus_client.make_wsgi_app()
