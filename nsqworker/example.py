from nsqhandler import NSQHandler
import json
import time

class MyHandler(NSQHandler):
    def __init__(self, topic, channel):
        super(MyHandler, self).__init__(topic, channel)

        self.register_route('visit.start', self.visit_start)
        self.register_route('visit.end', self.visit_end)
        self.register_route('visit.end', self.send_visit_email)
        self.register_route('visit.end', self.send_visit_report)
        self.register_route('report.sent', self.announce_report_sent)

    def visit_start(self, message):
        visit_id = message.get('data:visit:id')
        self.logger.info("Visit started: {}".format(visit_id))
        self.send_message("test", json.dumps(dict(name="visit.end")))

    def visit_end(self, message):
        visit_id = message.get('data:visit:id')
        self.logger.info("Visit ended: {}".format(visit_id))

    def send_visit_email(self, message):
        self.logger.info("Sending email about visit")

    def send_visit_report(self, message):
        self.logger.info("Sending report about the visit")
        self.send_message("test", json.dumps(dict(name="report.sent")))

    def announce_report_sent(self, message):
        self.logger.info("The report has been sent")


class PingPong(NSQHandler):
    def __init__(self, topic, channel):
        super(PingPong, self).__init__(topic, channel)

        self.register_route("request.ping", self.ping)
        self.register_route("request.pong", self.pong)

    def ping(self, message):
        self.logger.info("Ping")
        time.sleep(1)
        self.send_message("test", json.dumps(dict(name="request.pong")))

    def pong(self, message):
        self.logger.info("Pong")
        time.sleep(1)
        self.send_message("test", json.dumps(dict(name="request.ping")))

import nsq

MyHandler(topic="test", channel="test")
PingPong("test", "pingpong")

nsq.run()
