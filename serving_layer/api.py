from flask import Flask, jsonify

import pandas as pd

from .service import get_batch_results
from .service import get_speed_layer_results
from .service import combine_results
import config

app = Flask(__name__)

@app.route('/batch_results')
def get_batch_result():
    """ API for returing query result """
    # batch_results = get_batch_results()
    # speed_results = get_speed_layer_results()
    
    # combined_results = combine_results(batch_results, speed_results)
    error_counts, average_resynch_counts, top5_dates, smallest_appbusy_node, earliest_fatal_kernel_date = get_batch_results()
    
    return jsonify({'error_counts': int(error_counts), 
                    'average_resynch_counts': average_resynch_counts, 
                    'top5_dates': top5_dates,
                    'smallest_appbusy_node': smallest_appbusy_node,
                    'earliest_fatal_kernel_date': earliest_fatal_kernel_date})

if __name__ == '__main__':
    app.run(debug=True)