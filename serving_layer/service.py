from pymongo import MongoClient
import pandas as pd
import config


def get_batch_results():
    """ Get Batch Processing Result """
    client = MongoClient(config.MONGO_SERVER)
    db = client.bgl_logs
 
    error_counts = list(db.batch_layer_fatal_error_counts_10_11.find({}, {'_id': 0}))[0]['errors_count']
    
    average_resynch_counts = list(db.batch_layer_average_resynch_counts.find({}, {'_id': 0}))
          
    top5_dates = list(db.batch_layer_top5_dates.find({}, {'_id': 0}))
    
    smallest_appbusy_node = list(db.batch_layer_smallest_appbusy_node.find({}, {'_id': 0}))[0]
    
    earliest_fatal_kernel_date = list(db.batch_layer_earliest_fatal_kernel_date.find({}, {'_id': 0}))[0]['datetime']
    
    
    return error_counts, \
           average_resynch_counts, \
           top5_dates, \
           smallest_appbusy_node, \
           earliest_fatal_kernel_date

## TODO
def get_speed_layer_results():
    """ Get Speed Processing Result """
    client = MongoClient(config.MONGO_SERVER)
    db = client.bgl_logs
    result =  pd.DataFrame(list(db.speed_layer.find().order('window.end', -1).limit(1)))
    client.close()
    return result

## TODO
def combine_results():
    pass
