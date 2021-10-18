import logging

import connect
import json
from confluent_kafka import avro
import ccloud_lib
import re
from escapejson import escapejson
from pre_process import pre_process
import time
import datetime as dt
# p = re.compile('(?<!\\\\)\'')
# p2 = re.compile('(?<!\\\\):')
last_time = dt.datetime.today().timestamp()
diffs = []


def delivery_report(err, msg):

    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    # total_messages
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    # else:
    #     # total_messages += 1
    #     print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def main():
    last_time = dt.datetime.today().timestamp()
    diffs = []
    total_messages = 0
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(message)s')
    producer = connect.get_producer()
    response = connect.get_live_tweets()
    for line in response.iter_lines():
        clean_json = None
        try:
            clean_json = pre_process(line)
        except Exception as e:
            logging.log(logging.ERROR, e)
        if clean_json is None:
            continue
        key = clean_json["key"]
        value = clean_json["value"]
        tweet = ccloud_lib.Tweet(tweet=escapejson(value.__str__().replace("\'", "\"")))
        user = ccloud_lib.Key(id=escapejson(key.__str__().replace("\'", "\"")))
        try:
            producer.produce(topic='tweets', value=tweet, key=user, on_delivery=delivery_report)
            producer.flush()
            new_time = dt.datetime.today().timestamp()
            diffs.append(new_time - last_time)
            last_time = new_time
            total_messages += 1
            if total_messages % 10 == 0:
                diffs = diffs[-10:]
                logging.log(logging.INFO, f"messages delivered per second:{len(diffs) / sum(diffs)}")

        except Exception as e:
            pass
            # logging.log(logging.ERROR, e)
            # logging.log(logging.INFO, value)
            # exit(-1)
    # exit()


if __name__ == "__main__":
    main()
