import multiprocessing
from data_ingestion.kafka_producer import produce_logs
from speed_layer.stream_processor import process_stream
from serving_layer.api import app

def run_batch_processing():
     batch_processing("hdfs://localhost:54310/bgl/BGL.log", "hdfs://localhost:54310/bgl/batch/results/01")

def run_kafka_producer():
    produce_logs("localhost:9092", "bgl_logs")

def run_stream_processor():
    process_stream("localhost:9092", "bgl_logs")

def run_api():
    app.run(debug=True)

if __name__ == "__main__":
    
    # Start Batch Processing
    # batch_process = multiprocessing.Process(target=run_batch_processing)
    # batch_process.start()
    
    # Start Kafka producer
    kafka_process = multiprocessing.Process(target=run_kafka_producer)
    kafka_process.start()

    # Start stream processor
    stream_process = multiprocessing.Process(target=run_stream_processor)
    stream_process.start()

    # Start API
    api_process = multiprocessing.Process(target=run_api)
    api_process.start()

    # Wait for all processes to complete
    kafka_process.join()
    stream_process.join()
    api_process.join()
    # batch_process.join()