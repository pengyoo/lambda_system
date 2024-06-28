from utils.spark_utils import *
import config

def append_to_mongo(df, epoch_id, mongo_document):
    """ Append processing result to mongodb """
    df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", mongo_document) \
        .save()


def overwrite_to_mongo(df, epoch_id, mongo_document):
    """ Overwrite processing result to mongodb """
    df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", mongo_document) \
        .save()


def process_stream(kafka_bootstrap_servers, kafka_topic):
    """ Process streaming logs """
    spark = create_spark_session("BGLStreamProcessor")
    
    kafka_df = get_kafka_df(spark, kafka_bootstrap_servers, kafka_topic)
    
    parsed_df = parse_logs(kafka_df)
    
    # Perform streaming analysis: 1
    error_counts_df = analyze_error_counts(parsed_df, spark)
    error_counts_query = error_counts_df \
        .writeStream \
        .foreachBatch(lambda df, epoch_id : append_to_mongo(df, epoch_id, config.MONGO_SPEED_BASEURI+"fatal_error_counts_10_11")) \
        .outputMode("complete") \
        .trigger(processingTime="1 minute") \
        .start()

    
    # Perform batch analysis: 5,
    average_resynch_df = analyze_average_resynch_counts(parsed_df, spark)
    average_resynch_query = average_resynch_df \
        .writeStream \
        .foreachBatch(lambda df, epoch_id : append_to_mongo(df, epoch_id, config.MONGO_SPEED_BASEURI+"average_resynch_counts")) \
        .outputMode("complete") \
        .trigger(processingTime="1 minute") \
        .start()
        
    
    # # Perform batch analysis: 9
    top5_dates_df = analyze_top5_dates(parsed_df, spark)
    top5_dates_query = top5_dates_df \
        .writeStream \
        .foreachBatch(lambda df, epoch_id : append_to_mongo(df, epoch_id, config.MONGO_SPEED_BASEURI+"top5_dates")) \
        .outputMode("complete") \
        .trigger(processingTime="1 minute") \
        .start()
        
    
    # # Perform batch analysis: 15
    smallest_appbusy_node_df = analyze_smallest_appbusy_node(parsed_df, spark)
    smallest_appbusy_node_query = smallest_appbusy_node_df \
        .writeStream \
        .foreachBatch(lambda df, epoch_id : append_to_mongo(df, epoch_id, config.MONGO_SPEED_BASEURI+"smallest_appbusy_node")) \
        .outputMode("complete") \
        .trigger(processingTime="1 minute") \
        .start()
        
    
    # Streaming analysis doesn't need to process 18 question, as it required the earliest date
    
    error_counts_query.awaitTermination()
    average_resynch_query.awaitTermination()
    top5_dates_query.awaitTermination()
    smallest_appbusy_node_query.awaitTermination()

if __name__ == "__main__":
    process_stream(config.KAFKA_SERVER, config.KAFKA_TOPIC)