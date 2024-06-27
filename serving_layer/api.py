from flask import Flask, jsonify
from pymongo import MongoClient
import pandas as pd
from utils.spark_utils import create_spark_session

app = Flask(__name__)

def get_batch_results(path):
    # spark = create_spark_session("BGLServingLayer")
    # return spark.read.parquet(path).toPandas()
    client = MongoClient("mongodb://192.168.86.1:27017/")
    db = client.bgl_logs
    return pd.DataFrame(list(db.batch_layer.find()))

def get_speed_layer_results():
    client = MongoClient("mongodb://192.168.86.1:27017/")
    db = client.bgl_logs
    return pd.DataFrame(list(db.speed_layer.find()))

def combine_results(batch_df, speed_df): 
    
    total_errors_count = batch_df['errors_count'].sum() + speed_df['errors_count'].max()
    
    return total_errors_count

@app.route('/error_counts')
def get_error_counts():
    batch_results = get_batch_results("hdfs://localhost:54310/bgl/batch/results/01")
    speed_results = get_speed_layer_results()
    
    combined_results = combine_results(batch_results, speed_results)
    
    return jsonify({'error_counts':int(combined_results)})

if __name__ == '__main__':
    app.run(debug=True)