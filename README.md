# kafka-with-avro

## This is a boilerplate project for using kafka with avro schema registry


### template of ccloud_config

```
# Kafka
bootstrap.servers=<your bootstrap server>
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=<cluster api key>
sasl.password=<cluster api secret>

# Confluent Cloud Schema Registry
schema.registry.url=<your schema registry url>
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=<your api key>:<your api secret>
```


### template of twitter_config

```
CONSUMER_KEY=<your consumer key>
ACCESS_TOKEN=<your access token>
ACCESS_SECRET=<your access secret>
CONSUMER_SECRET=<your consumer secret>
```
