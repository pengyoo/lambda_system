from kafka import KafkaProducer
import json
import time
import random
import config

def produce_logs(bootstrap_servers, topic):
    """ Simulate an application that is producing logs constantly """
    
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    # - 1719565562 2024-06-27 R39-M19-N50-C:J29-U93 2024-06-28-09.11.06.367218 R39-M19-N50-C:J29-U93 RAS KERNEL ERROR minor error
    # - 1719565920 2024-06-27 R94-M33-N29-C:J62-U81 2024-06-27-09.31.31.743136 R94-M33-N29-C:J62-U81 RAS KERNEL INFO network issue
    while True:
        # Generate random
        timestamp = int(time.time())
        datetime = time.strftime("%Y-%m-%d-%H.%M.%S", time.localtime(time.time() - random.randint(0, 100000)))
        date = datetime[0:10]
        node = f"R{random.randint(0, 99):02}-M{random.randint(0, 99):02}-N{random.randint(0, 99):02}-C:J{random.randint(0, 99):02}-U{random.randint(0, 99):02}"
        log_level = random.choice(['INFO', 'WARNING', 'ERROR', 'FATAL'])
        message_content = random.choice(['major internal error', 'minor error', 'network issue', 'disk full'])

        log_entry = f"- {timestamp} {date} {node} {datetime}.{str(random.randint(100000, 999999))} {node} RAS KERNEL {log_level} {message_content}"

        producer.send(topic, log_entry)
        time.sleep(1)
        # print(log_entry)

if __name__ == "__main__":
    produce_logs(config.KAFKA_SERVER, config.KAFKA_TOPIC)