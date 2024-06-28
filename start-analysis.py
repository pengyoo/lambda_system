import multiprocessing
from batch_layer.batch_processor import batch_processing
from speed_layer.stream_processor import process_stream
from data_ingestion.kafka_producer import produce_logs
from data_ingestion.log_app import produce_file_logs

import config

def run_batch_processing():
    batch_processing(config.BATCH_LOG_PATH)

def run_stream_processor():
    process_stream(config.KAFKA_SERVER, config.KAFKA_TOPIC)

def run_kafka_producer():
    produce_logs(config.KAFKA_SERVER, config.KAFKA_TOPIC)

def run_log_app():
    produce_file_logs("/home/hadoop/logs/app.log")


if __name__ == "__main__":
    
    # Start Batch Processing
    # batch_process = multiprocessing.Process(target=run_batch_processing)
    # batch_process.start()

    # Start stream processor
    stream_process = multiprocessing.Process(target=run_stream_processor)
    stream_process.start()

    
    # # Start Kafka producer: Simulate an application producing logs 
    # kafka_process = multiprocessing.Process(target=run_kafka_producer)
    # kafka_process.start()
    
    
    # Start log_app: Simulate an application writing logs to file and then Flume will send it to Kafka 
    logapp_process = multiprocessing.Process(target=run_log_app)
    logapp_process.start()
    
    # Wait for all processes to complete
    stream_process.join()
    # batch_process.join()
    logapp_process.join()
    # kafka_process.join()