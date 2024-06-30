from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd

from .service import get_batch_results
from .service import get_speed_layer_results
from .service import combine_results
import config

app = Flask(__name__)
CORS(app)

@app.route('/batch_results')
def get_batch_result():
    """ API for returing query result """
    # batch_results = get_batch_results()
    speed_results = get_speed_layer_results()
    
    # combined_results = combine_results(batch_results, speed_results)
    errors_count, average_resynch_counts, top5_dates, smallest_appbusy_node, earliest_fatal_kernel_date = get_batch_results()
    
    return jsonify({'errors_count': int(errors_count), 
                    'average_resynch_counts': average_resynch_counts, 
                    'top5_dates': top5_dates,
                    'smallest_appbusy_node': smallest_appbusy_node,
                    'earliest_fatal_kernel_date': earliest_fatal_kernel_date})


@app.route('/realtime_results')
def get_realtime_result():
    """ API for returing query result """
    speed_results = get_speed_layer_results()
    batch_results = get_batch_results()
    
    combined_results = combine_results(batch_results, speed_results)
    errors_count, total_resynch_counts, top5_dates, smallest_appbusy_node, earliest_fatal_kernel_date = combined_results
    
    return jsonify({'errors_count': int(errors_count), 
                    'total_resynch_counts': total_resynch_counts, 
                    'top5_dates': top5_dates,
                    'smallest_appbusy_node': smallest_appbusy_node,
                    'earliest_fatal_kernel_date': earliest_fatal_kernel_date})

if __name__ == '__main__':
    app.run(debug=True)