from confluent_kafka.avro import SerializerError
import connect


def consume():
    total_count = 0
    consumer = connect.get_consumer()
    while True:
        try:
            msg = consumer.poll()
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                key = msg.key()
                tweet = msg.value()
                tweet = tweet.tweet
                total_count += 1
                print("Consumed record with key {} and value {},\nAnd updated total count to {}"
                      .format(key.id, tweet, total_count))
        except KeyboardInterrupt:
            break
        except SerializerError as e:
            # Report malformed record, discard results, continue polling
            print("Message deserialization failed {}".format(e))
            pass

            # Leave group and commit final offsets
    consumer.close()


if __name__ == "__main__":
    consume()
