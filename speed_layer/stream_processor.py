from pyspark.sql.functions import col, month, count, window
from utils.spark_utils import create_spark_session, get_kafka_df, parse_logs
import config

def speed_layer_processing(df):
    """ Process logs """
    return df \
        .filter((col("level") == "FATAL") & 
                (month(col("datetime")).isin(6, 10, 11)) & 
                (col("message_content").contains("major internal error"))) \
        .groupBy(window(col("datetime"), "1 hour")) \
        .agg(count("*").alias("errors_count"))

def write_to_mongo(df, epoch_id):
    """ Write processing result to mongodb"""
    df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", config.MONGO_SPEED_URI) \
        .save()

def process_stream(kafka_bootstrap_servers, kafka_topic):
    """ Process streaming logs """
    spark = create_spark_session("BGLStreamProcessor")
    
    kafka_df = get_kafka_df(spark, kafka_bootstrap_servers, kafka_topic)
    parsed_df = parse_logs(kafka_df)
    
    query = speed_layer_processing(parsed_df) \
        .writeStream \
        .foreachBatch(write_to_mongo) \
        .outputMode("update") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    process_stream(config.KAFKA_SERVER, config.KAFKA_TOPIC)