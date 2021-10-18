import pprint
from time import sleep
import logging

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer, AvroConsumer
import requests
import requests_oauthlib
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
import ccloud_lib

CCONFIG_FILE = 'ccloud_config.txt'
TWITTER_CONFIG = 'twitter_config.txt'


def initiate_connection(args: dict):
    return requests_oauthlib.OAuth1(args.get("CONSUMER_KEY"), args.get("CONSUMER_SECRET")
                                    , args.get("ACCESS_TOKEN"), args.get("ACCESS_SECRET"))


def get_tweets(oauth: requests_oauthlib.OAuth1):
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=oauth, stream=True)
    print(query_url, response)
    return response


def get_producer():
    config = ccloud_lib.read_ccloud_config(CCONFIG_FILE)
    schema_registry_conf = {
        'url': config['schema.registry.url'],
        'basic.auth.user.info': config['basic.auth.user.info']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    key_avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client,
                                         schema_str=ccloud_lib.key_schema,
                                         to_dict=ccloud_lib.Key.to_dict)
    value_avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client,
                                           schema_str=ccloud_lib.value_schema,
                                           to_dict=ccloud_lib.Tweet.to_dict)
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(config)
    producer_conf['key.serializer'] = key_avro_serializer
    producer_conf['value.serializer'] = value_avro_serializer
    producer = SerializingProducer(producer_conf)
    return producer


def get_consumer():
    topic = "tweets"
    conf = ccloud_lib.read_ccloud_config(CCONFIG_FILE)

    schema_registry_conf = {
        'url': conf['schema.registry.url'],
        'basic.auth.user.info': conf['basic.auth.user.info']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    key_avro_deserializer = AvroDeserializer(schema_str=ccloud_lib.key_schema,
                                             schema_registry_client=schema_registry_client,
                                             from_dict=ccloud_lib.Key.dict_to_key)
    value_avro_deserializer = AvroDeserializer(schema_str=ccloud_lib.value_schema,
                                               schema_registry_client=schema_registry_client,
                                               from_dict=ccloud_lib.Tweet.dict_to_tweet)
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['key.deserializer'] = key_avro_deserializer
    consumer_conf['value.deserializer'] = value_avro_deserializer
    consumer_conf['group.id'] = 'python_example_group_2'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = DeserializingConsumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])
    return consumer


def get_live_tweets():
    confg = ccloud_lib.read_ccloud_config(TWITTER_CONFIG)
    o_auth = initiate_connection(confg)
    return get_tweets(o_auth)


