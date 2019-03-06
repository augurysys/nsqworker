import os
import requests
import logging
from time import sleep


def nsq_config_from_env():

    concurrency = int(os.environ.get("NSQ_CONCURRENCY", "1"))
    max_in_flight = int(os.environ.get("NSQ_MAX_IN_FLIGHT", "1"))

    return {
        "concurrency": concurrency,
        "max_in_flight": max_in_flight
    }


# Create NSQ topics
def register_nsq_topics(nsqd_http_hosts, topic_names):
    topic_hosts = []
    for host in nsqd_http_hosts:
        for topic in topic_names:
            topic_hosts.append((host, topic))

    while len(topic_hosts) > 0:
        topic_hosts[:] = [th for th in topic_hosts if not post_topic(*th)]
        sleep(1)


def post_topic(nsq_http, topic):
    try:
        res = requests.post(
            "http://" + nsq_http + "/topic/create?topic="+str(topic), data="", timeout=1
        )
        if res.status_code != 200:
            logging.warning(
                "Bad response for creating {} topic: {}".format(topic, str(res.status_code))
            )
        else:
            logging.warning("topic {} created successfully on nsqd {}".format(topic, nsq_http))
            return True

    except Exception as e:
        logging.warning(
            "got HTTP Error while trying to create topic {}, err msg: {}".format(
                str(topic), str(e)
            )
        )
        return False
