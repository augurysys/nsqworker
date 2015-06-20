from nsqwriter import NSQWriter


class NSQRequestor(NSQWriter):
    def __init__(self):
        super(NSQRequestor, self).__init__()

    def make_request(self, topic, message, wait_for_response=False):
        # If wait for response is enabled, open a random topic/channel

        # Send the request to the specified topic
        print "Sending message"
        self.send_message(topic, message)

        # If wait for response is enabled, open an nsqreader for that topic (random channel), then wait for an answer

        # If wait for response is enabled, close the topic/channel you opened before


if __name__ == "__main__":
    import json

    import nsq

    requestor = NSQRequestor()

    # request = dict(name="request.ping")
    # requestor.make_request("test", json.dumps(request))

    # response = requestor.make_request(json.dumps(request), wait_for_response=True)
    # print response

    import requests

    request = requests.post("http://localhost:4151/pub?topic=test", data=json.dumps(dict(name="request.ping")))

    print request
    # nsq.run()

# TODO - message encryption
# TODO - loglevel setting of nsqhandler (to be like ThreadWorker)
# TODO - host_pool like in the nsq example