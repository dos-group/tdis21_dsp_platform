from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "wgs-alarms",
    # TODO: Provide your server IP / hostname here
    bootstrap_servers='localhost:32090',
    # TODO: Authentication here
)

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value.decode("utf-8")))
