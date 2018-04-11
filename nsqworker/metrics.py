import prometheus_client

NSQ_MESSAGES_SENT_COUNTER = prometheus_client.Counter(
    'nsq_messages_sent_count',
    'number of messages sent to topic',
    ['topic'])
NSQ_MESSAGES_RETRY_COUNTER = prometheus_client.Counter(
    'nsq_messages_retry_count',
    'number of messages retry by topic',
    ['name'])
NSQ_RECEIVED_MESSAGES_HISTOGRAM = prometheus_client.Histogram(
    'nsq_received_messages',
    'histogram of received messages',
    ['topic', 'channel', 'status', 'event'])

NSQ_MESSAGES_SENT_COUNTER.labels('x').inc()


def start_metrics_server(port):
    prometheus_client.start_http_server(port)
