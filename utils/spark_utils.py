from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, unix_timestamp, concat_ws, expr, month, to_date, count, window

def create_spark_session(app_name="BGLLogAnalysis"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()


def stop_spark_session(spark):
    spark.stop()


def get_kafka_df(spark, kafka_bootstrap_servers, topic):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic) \
        .load()


def parse_logs(df):
        
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