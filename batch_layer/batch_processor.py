
import config
from utils.spark_utils import *


def batch_processing(input_path):
    """ Process historical (batch) logs """
    print("#######################  Batch Processing Started  ######################")
    
    spark = create_spark_session("BGLBatchProcessor")
    
    # Read historical data
    historical_data = spark.read.text(input_path)
    
    # Parse log
    bgl_df = parse_logs(historical_data)
    
    # Perform batch analysis: 1
    result_df = analyze_error_counts(bgl_df, spark)
    write_batch_to_mongo(result_df, "fatal_error_counts_10_11")
    
    # Perform batch analysis: 5
    average_df = analyze_total_resynch_counts_by_month(bgl_df, spark)
    write_batch_to_mongo(average_df, "total_resynch_counts")
    
    # Perform batch analysis: 9
    top5_dates_df = analyze_top5_dates(bgl_df, spark)
    write_batch_to_mongo(top5_dates_df, "top5_dates")
    
    # Perform batch analysis: 15
    smallest_appbusy_node_df = analyze_smallest_appbusy_node(bgl_df, spark)
    write_batch_to_mongo(smallest_appbusy_node_df, "smallest_appbusy_node")
    
    # Perform batch analysis: 18
    earliest_fatal_kernel_date_df = analyze_earliest_fatal_kernel_date(bgl_df, spark)
    write_batch_to_mongo(earliest_fatal_kernel_date_df, "earliest_fatal_kernel_date")
    
    # Perform level frequency analysis
    level_frequency = analyze_level(bgl_df, spark)
    write_batch_to_mongo(level_frequency, "level_frequency")
    
    # Perform node fatal analysis
    fatal_node = analyze_fatal_node(bgl_df, spark)
    write_batch_to_mongo(fatal_node, "fatal_node_frequency")
    
     # Perform message content analysis
    message_content_reslut = analyze_message_content(bgl_df, spark)
    write_batch_to_mongo(message_content_reslut, "message_content_frequency")
    
    print("#######################  Batch Processing Finished  ######################")
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    batch_processing(config.BATCH_LOG_PATH)
