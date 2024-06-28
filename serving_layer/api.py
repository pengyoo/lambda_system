from flask import Flask, jsonify

import pandas as pd
from utils.spark_utils import create_spark_session
from utils.spark_utils import get_batch_results
from utils.spark_utils import get_speed_layer_results
from utils.spark_utils import combine_results
import config

app = Flask(__name__)

@app.route('/error_counts')
def get_error_counts():
    """ API for returing query result """
    batch_results = get_batch_results()
    speed_results = get_speed_layer_results()
    
    combined_results = combine_results(batch_results, speed_results)
    
    return jsonify({'error_counts':int(combined_results)})

if __name__ == '__main__':
    app.run(debug=True)