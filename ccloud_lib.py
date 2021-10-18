#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Helper module
#
# =============================================================================

import argparse, sys
import json

from confluent_kafka import avro, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from uuid import uuid4
from dataclasses import dataclass


KEY_SCHEMA_FILE = "./schemas/key_schema.avsc"
VALUE_SCHEMA_FILE = "./schemas/value_schema.avsc"
key_schema = ""
value_schema = ""
with open(KEY_SCHEMA_FILE) as sf:
    for line in sf:
        key_schema += line
    sf.close()
with open(VALUE_SCHEMA_FILE) as sf:
    for line in sf:
        value_schema += line
    sf.close()


@dataclass
class Key(object):
    id: str

    @staticmethod
    def dict_to_key(obj, ctx):
        return Key(id=obj.__str__())

    @staticmethod
    def key_to_dict(obj, ctx):
        return Key.to_dict(obj)

    def to_dict(self, ctx) -> dict:
        """
        :return dictionary representation of the key object:
        """
        key = json.loads(self.id)
        # for k in key:
        #     if key[k] is None:
        #         key[k] = ""
        # key = {"id": self.id}
        return key


@dataclass
class Tweet(object):
    tweet: str

    @staticmethod
    def dict_to_tweet(obj, ctx):
        return Tweet(tweet=obj.__str__())

    @staticmethod
    def tweet_to_dict(obj, ctx):
        return Tweet.to_dict(obj)

    def to_dict(self, ctx) -> dict:
        """
        :return: dictionary representation of tweet object
        """
        tweet = json.loads(self.tweet)
        # for k in tweet:
        #     if tweet[k] is None:
        #         tweet[k] = ""
        return tweet


class Name(object):
    """
        Name stores the deserialized Avro record for the Kafka key.
    """

    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["name", "id"]

    def __init__(self, name=None):
        self.name = name
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    @staticmethod
    def dict_to_name(obj, ctx):
        return Name(obj['name'])

    @staticmethod
    def name_to_dict(name, ctx):
        return Name.to_dict(name)

    def to_dict(self):
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        return dict(name=self.name)


class Count(object):
    """
        Count stores the deserialized Avro record for the Kafka value.
    """

    # Use __slots__ to explicitly declare all data members.
    __slots__ = ["count", "id"]

    def __init__(self, count=None):
        self.count = count
        # Unique id used to track produce request success/failures.
        # Do *not* include in the serialized object.
        self.id = uuid4()

    @staticmethod
    def dict_to_count(obj, ctx):
        return Count(obj['count'])

    @staticmethod
    def count_to_dict(count, ctx):
        return Count.to_dict(count)

    def to_dict(self):
        """
            The Avro Python library does not support code generation.
            For this reason we must provide a dict representation of our class for serialization.
        """
        return dict(count=self.count)


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
             description="Confluent Python Client example to produce messages \
                  to Confluent Cloud")

    parser._action_groups.pop()

    required = parser.add_argument_group('required arguments')
    required.add_argument('-f',
                          dest="config_file",
                          help="path to Confluent Cloud configuration file",
                          required=True)
    required.add_argument('-t',
                          dest="topic",
                          help="topic name",
                          required=True)
    args = parser.parse_args()

    return args


def read_ccloud_config(config_file):
    """
    Read Confluent Cloud configuration for librdkafka clients
    """

    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    #conf['ssl.ca.location'] = certifi.where()
    return conf


def pop_schema_registry_params_from_config(conf):
    """
    Remove potential Schema Registry related configurations from dictionary
    """

    conf.pop('schema.registry.url', None)
    conf.pop('basic.auth.user.info', None)
    conf.pop('basic.auth.credentials.source', None)

    return conf


def create_topic(conf, topic):
    """
    Create a topic if needed
    Examples of additional admin API functionality:
    https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/adminapi.py
    """

    admin_client_conf = pop_schema_registry_params_from_config(conf.copy())
    a = AdminClient(admin_client_conf)

    fs = a.create_topics([NewTopic(
         topic,
         num_partitions=1,
         replication_factor=3
    )])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
            # Otherwise fail fast
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                print("Failed to create topic {}: {}".format(topic, e))
                sys.exit(1)