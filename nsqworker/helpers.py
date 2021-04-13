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
            logging.info("topic {} created successfully on nsqd {}".format(topic, nsq_http))
            return True

    except Exception as e:
        logging.warning(
            "got HTTP Error while trying to create topic {}, err msg: {}".format(
                str(topic), str(e)
            )
        )
        return False


def _discover_nsqd(nsq_topic, lookupd_http_addresses=None):
    nsqd_nodes_list = list()
    environment_nsqd_tcp_addresses = os.getenv("NSQD_DATA_SERVICES_TCP_ADDRESSES")
    if not lookupd_http_addresses:
        environment_lookupd = os.getenv("LOOKUPD_HTTP_ADDRESSES")
        lookupd_http_addresses = environment_lookupd if lookupd_http_addresses else ""
    lookupds_list = lookupd_http_addresses.split(",")

    for lookup_endpoint in lookupds_list:
        producers_result = requests.get(f"http://{lookup_endpoint}/nodes")
        producers_list = producers_result.json().get('producers')
        for producer_dict in producers_list:
            produced_topics_list = producer_dict.get("topics")
            if nsq_topic in produced_topics_list:
                nsqd_address = producer_dict.get("broadcast_address")
                nsqd_tcp_port = producer_dict.get("tcp_port")
                nsqd_host = f"{nsqd_address}:{nsqd_tcp_port}"
                nsqd_nodes_list.append(nsqd_host)
    if len(nsqd_nodes_list) == 0:
        print(f"Found no nsqd that hold the topic {nsq_topic}, defaulting to {environment_nsqd_tcp_addresses}")
        nsqd_nodes_list = str(environment_nsqd_tcp_addresses).split(",")
    else:
        print(f"Found the following nsq nodes: {nsqd_nodes_list}")
    # Remove empty list members
    clean_nsqd_nodes_list = list(filter(lambda node: node not in [None, "None", ""],
                                        nsqd_nodes_list))
    return clean_nsqd_nodes_list


def random_nsqd_selector(nsq_topic, lookupd_http_addresses=None):
    nsqd_nodes_list = discover_nsqd(nsq_topic=nsq_topic,
                                    lookupd_http_addresses=lookupd_http_addresses)
    nsqd_host = random.choice(nsqd_nodes_list)
    print(f"Selected random nsqd node: {nsqd_host}")
    return nsqd_host


def _post_using_requests(url, data):
    return requests.post(url=url, data=data)


def post_message_to_nsq(nsqd_http_address, topic, message_payload):
    # Build post url
    post_url = f"http://{nsqd_http_address}/pub?topic={topic}"
    try:
        post_result = _post_using_requests(url=post_url, data=message_payload)
        external_logger.info(f"Published message: {message_payload}"[:100])
        return post_result
    except Exception as e:
        external_logger.error(f"Failed to post result to {post_url}, Exception: {e}")
        return None
