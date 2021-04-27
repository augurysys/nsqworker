import os
import requests
import logging
import random
from time import sleep

NSQ_TOPIC_EXISTS = True
NSQ_TOPIC_DOESNT_EXISTS = False

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
            "http://" + nsq_http + "/topic/create?topic=" + str(topic), data="", timeout=1
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


def random_nsqd_node_selector(nsq_topic, lookupd_http_addresses=None, environment_nsqd_tcp_addresses=None):
    if not lookupd_http_addresses:
        raise Exception("Missing mandatory lookupd_http_addresses parameter")
    if not environment_nsqd_tcp_addresses:
        raise Exception("Missing mandatory environment_nsqd_tcp_addresses parameter")
    nsqd_nodes, topic_exists = _discover_nsqd_nodes(nsq_topic=nsq_topic,
                                                    lookupd_http_addresses=lookupd_http_addresses,
                                                    environment_nsqd_tcp_addresses=environment_nsqd_tcp_addresses)
    nsqd_node_tcp = random.choice(nsqd_nodes)
    logging.info(f"Selected random nsqd node: {nsqd_node_tcp}")
    nsqd_node_http = nsqd_node_tcp.replace("4150", "4151")
    if not topic_exists:
        logging.warning(f"Topic [{nsq_topic}] doesn't exist - please create it.")
    return nsqd_nodes, nsqd_node_http


def _discover_nsqd_nodes(nsq_topic, lookupd_http_addresses, environment_nsqd_tcp_addresses):
    nsqd_nodes = list()
    topic_existence = None
    lookupds_endpoints = lookupd_http_addresses.split(",")

    for lookup_endpoint in lookupds_endpoints:
        producers_result = requests.get(f"http://{lookup_endpoint}/nodes")
        producers_list = producers_result.json().get('producers')
        for producer_dict in producers_list:
            produced_topics_list = producer_dict.get("topics")
            if nsq_topic in produced_topics_list:
                nsqd_address = producer_dict.get("broadcast_address")
                nsqd_tcp_port = producer_dict.get("tcp_port")
                nsqd_host = f"{nsqd_address}:{nsqd_tcp_port}"
                nsqd_nodes.append(nsqd_host)
    if len(nsqd_nodes) == 0:
        logging.warning(f"Found no nsqd that holds the topic {nsq_topic}, defaulting to {environment_nsqd_tcp_addresses}")
        nsqd_nodes = str(environment_nsqd_tcp_addresses).split(",")
        topic_existence = NSQ_TOPIC_DOESNT_EXISTS
    else:
        logging.info(f"Found the following nsq nodes: {nsqd_nodes}")
        topic_existence = NSQ_TOPIC_EXISTS
    nsqd_nodes = _remove_empty_values_from_list(list_of_values=nsqd_nodes)
    nsqd_nodes = _remove_duplicate_values_from_list(list_of_values=nsqd_nodes)
    return nsqd_nodes, topic_existence


def _remove_empty_values_from_list(list_of_values):
    if not isinstance(list_of_values, list):
        raise Exception(f"Input must be a list, got {type(list_of_values)} instead")
    return list(filter(lambda element: element not in [None, "None", ""], list_of_values))


def _remove_duplicate_values_from_list(list_of_values):
    if not isinstance(list_of_values, list):
        raise Exception(f"Input must be a list, got {type(list_of_values)} instead")
    return list(dict.fromkeys(list_of_values))




def _post_using_requests(url, data):
    return requests.post(url=url, data=data)


def post_message_to_nsq(nsqd_http_address, topic, message_payload):
    # Build post url
    post_url = f"http://{nsqd_http_address}/pub?topic={topic}"
    try:
        post_result = _post_using_requests(url=post_url, data=message_payload)
        logging.info(f"Published message: {message_payload}"[:100])
        return post_result
    except Exception as e:
        logging.error(f"Failed to post result to {post_url}, Exception: {e}")
        return None
