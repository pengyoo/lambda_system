from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, unix_timestamp, concat_ws, expr, year, month, to_date, count, window, second, slice
import config

def create_spark_session(app_name="BGLLogAnalysis"):
    """ Create Spark Session """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()


def get_kafka_df(spark, kafka_bootstrap_servers, topic):
    """ Get Stream DF from Kafka """
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .load()


def parse_logs(df):
    """ Preprocess Logs """    
    # Split the text into columns based on spaces
    bgl_df = df.withColumn('split', split(col('value'), ' '))

    # Extract the necessary columns and properly combine the MessageContent
    bgl_df = bgl_df.select(
        col('split').getItem(0).alias('flag'),
        col('split').getItem(1).cast('long').alias('timestamp'),
        col('split').getItem(2).alias('date'),
        col('split').getItem(3).alias('node'),
        col('split').getItem(4).alias('datetime'),
        col('split').getItem(6).alias('message_type'),
        col('split').getItem(7).alias('system_component'),
        col('split').getItem(8).alias('level'),
        expr("slice(split(value, ' '), 10, size(split(value, ' ')))").alias('message_content_array')
    )

    # Combine the message_content_array into message_content string
    bgl_df = bgl_df.withColumn('message_content', concat_ws(' ', col('message_content_array')))

    # Convert datetime string to timestamp
    bgl_df = bgl_df.withColumn('datetime', unix_timestamp(col('datetime'), 'yyyy-MM-dd-HH.mm.ss.SSSSSS').cast('timestamp'))

    # Filter out unnecessary columns
    bgl_df = bgl_df.select('flag', 'timestamp', 'date', 'node', 'datetime',  'message_type', 'system_component', 'level', 'message_content')

    return bgl_df



def analyze_error_counts(bgl_df, spark):
    """ 1. How many fatal log entries in the months of October or November resulted from a ”major internal error”?  """
    # filtered_df = bgl_df \
    #             .filter(
    #                 (col('level') == 'FATAL') &
    #                 (month(col('datetime')).isin(10, 11)) &
    #                 (col('message_content').contains('major internal error'))
    #             )
    
    # # Aggregate to count the number of such log entries
    # result_df = filtered_df \
    #             .groupBy() \
    #             .agg(count("*") \
    #             .alias("errors_count"))
                
     # Create temp view
    bgl_df.createOrReplaceTempView("bgl_logs")
    
    # Query
    result_df = spark.sql("""
              SELECT 
                count(*) as errors_count
              FROM 
                bgl_logs
              WHERE 
                level = 'FATAL' 
                AND month(datetime) in (10, 11) 
                AND message_content like '%major internal error%'
              """)
    
    
    return result_df


def analyze_total_resynch_counts_by_month(bgl_df, spark):
    """ 5. For each month, what is the total number of ”re-synch state events” that occurred?  """
    
    resynch_df = bgl_df.withColumn('year_month', concat_ws("-", year("datetime"), month("datetime")))
    
    # Create temp view
    resynch_df.createOrReplaceTempView("bgl_logs")

    # Query
    # result_df = spark.sql("""
    #     SELECT 
    #         year_month,
    #         count(*) AS total_resynch_counts
    #     FROM 
    #         bgl_logs
    #     WHERE 
    #         message_content LIKE '%re-synch state%'
    #     GROUP BY 
    #         year_month
    # """)
    
    result_df = spark.sql("""
        SELECT 
            year_month,
            SUM(CASE WHEN message_content LIKE '%re-synch state%' THEN 1 ELSE 0 END) AS total_resynch_counts
        FROM 
            bgl_logs
        GROUP BY 
            year_month
        ORDER BY 
            year_month
                          """)
    return result_df


def analyze_top5_dates(bgl_df, spark):
    
    """ 9. What are the top 5 most frequently occurring dates in the log?  """
    # top5_dates = bgl_df \
    #                 .groupby('date') \
    #                 .agg(count('*') \
    #                 .alias('count'))
    # top5_dates = top5_dates \
    #                 .orderBy(top5_dates['count'].desc()) \
    #                 .head(5)
    # top5_dates_df = spark.createDataFrame(top5_dates)
    
    # Create temp view
    bgl_df.createOrReplaceTempView("bgl_logs")
    # Query
    top5_dates_df= spark.sql("""
                            SELECT 
                                date, count(*) as count
                            FROM
                                bgl_logs
                            GROUP BY
                                date
                            ORDER BY
                                count desc
                            LIMIT
                                5
                            """)
    
    return top5_dates_df


def analyze_smallest_appbusy_node(bgl_df, spark):
    
    """ 15. Which node generated the smallest number of APPBUSY events?  """
    # smallest_appbusy_node = bgl_df \
    #                         .filter((col('system_component') == 'APP') & col('message_content').contains('busy')) \
    #                         .groupby('node') \
    #                         .agg(count('*') \
    #                         .alias('app_busy_num'))
    # smallest_appbusy_node = smallest_appbusy_node \
    #                         .orderBy(smallest_appbusy_node['app_busy_num'].asc())
    # Create temp view
    bgl_df.createOrReplaceTempView("bgl_logs")
    # Query
    smallest_appbusy_df = spark.sql("""
                           SELECT 
                                node, count(*) as app_busy_num
                            FROM
                                bgl_logs
                            WHERE 
                                system_component = 'APP'
                                AND message_content like '%busy%'
                            GROUP BY
                                node
                            ORDER BY
                                app_busy_num asc
                            LIMIT
                                1
                            """)
    
    return smallest_appbusy_df


def analyze_earliest_fatal_kernel_date(bgl_df, spark):
    
    """ 18. On which date and time was the earliest fatal kernel error where the message contains ”Lustre mount FAILED”?  """
    # result_df =  bgl_df \
    #     .filter(col('message_content') \
    #     .contains('Lustre mount FAILED')) \
    #     .orderBy(bgl_df['datetime'].asc()) \
    #     .limit(1)
    
    # Create temp view
    bgl_df.createOrReplaceTempView("bgl_logs")
    # Query
    result_df = spark.sql("""
                            SELECT 
                                datetime
                            FROM
                                bgl_logs
                            WHERE
                                message_content like '%Lustre mount FAILED%'
                            ORDER BY
                                datetime asc
                            LIMIT
                                1
                         """)
    
    return result_df


def write_batch_to_mongo(result_df, mongo_document):
    """ Write processing result to mongodb"""
    result_df.write \
        .format("mongo") \
        .mode("overwrite") \
        .option("uri", config.MONGO_BATCH_BASEURI + mongo_document) \
        .save()