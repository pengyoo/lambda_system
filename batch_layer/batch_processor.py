
from pyspark.sql.functions import col, month, count, window
from utils.spark_utils import create_spark_session, parse_logs
import config

def batch_processing(input_path):
    """ Process historical (batch) logs """
    
    spark = create_spark_session("BGLBatchProcessor")
    
    # Read historical data
    historical_data = spark.read.text(input_path)
    
    # Parse log
    bgl_df = parse_logs(historical_data)
    
    print("#######################  Batch Processing Started  ######################")

    # Perform batch analysis
    result_df = analyze_error_counts(bgl_df)
                
    # Save results
    write_to_mongo(result_df)
    
    print("#######################  Batch Processing Finished  ######################")


def analyze_error_counts(bgl_df):
    filtered_df = bgl_df \
                .filter(
                    (col('level') == 'FATAL') &
                    (month(col('datetime')).isin(10, 11)) &
                    (col('message_content').contains('major internal error'))
                )
    
    # Aggregate to count the number of such log entries
    result_df = filtered_df \
                .groupBy() \
                .agg(count("*") \
                .alias("errors_count"))
    return result_df

def write_to_mongo(result_df):
    """ Write processing result to mongodb"""
    result_df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", config.MONGO_BATCH_URI) \
        .save()

if __name__ == "__main__":
    batch_processing(config.BATCH_LOG_PATH)
