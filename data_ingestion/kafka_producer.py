from kafka import KafkaProducer
import json
import time
import random

def produce_logs(bootstrap_servers, topic):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    while True:
        log_entry = "- 1117838571 2005.11.03 R02-M1-N0-C:J12-U11 2005-11-03-15.42.51.749199 R02-M1-N0-C:J12-U11 RAS KERNEL FATAL major internal error"

        producer.send(topic, log_entry)
        time.sleep(1)

if __name__ == "__main__":
    produce_logs("localhost:9092", "test")