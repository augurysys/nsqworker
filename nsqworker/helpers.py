import os

def nsq_config_from_env():
    
    concurrency = int(os.environ.get("NSQ_CONCURRENCY", "1"))
    max_in_flight = int(os.environ.get("NSQ_MAX_IN_FLIGHT", "1"))

    return {
        "concurrency": concurrency,
        "max_in_flight": max_in_flight
    }