
from pyspark.sql.functions import col, month, count, window
from utils.spark_utils import create_spark_session, parse_logs

def batch_processing(input_path, output_path):
    spark = create_spark_session("BGLBatchProcessor")
    
    # Read historical data
    historical_data = spark.read.text(input_path)
    
    # Parse log
    bgl_df = parse_logs(historical_data)
    
    print("#######################  Batch Processing Started  ######################")
    
    # Perform batch analysis
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
                
    
    
    
    # Save results
    result_df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", "mongodb://192.168.86.1:27017/bgl_logs.batch_layer") \
        .save()
    
    print("#######################  Batch Processing Finished  ######################")

if __name__ == "__main__":
    batch_processing("hdfs://localhost:54310/bgl/BGL.log", "hdfs://localhost:54310/bgl/batch/results/01")
