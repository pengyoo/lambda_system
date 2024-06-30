from pymongo import MongoClient
import pandas as pd
import config


def get_batch_results():
    """ Get Batch Processing Result """
    client = MongoClient(config.MONGO_SERVER)
    db = client.bgl_batch
 
    errors_count = list(db.fatal_error_counts_10_11.find({}, {'_id': 0}))[0]['errors_count']
    
    total_resynch_counts = list(db.total_resynch_counts.find({}, {'_id': 0}))
          
    top5_dates = list(db.top5_dates.find({}, {'_id': 0}))
    
    smallest_appbusy_node = list(db.smallest_appbusy_node.find({}, {'_id': 0}))
    
    earliest_fatal_kernel_date = list(db.earliest_fatal_kernel_date.find({}, {'_id': 0}))[0]['datetime']
    
    
    return errors_count, \
           total_resynch_counts, \
           top5_dates, \
           smallest_appbusy_node, \
           earliest_fatal_kernel_date

## TODO
def get_speed_layer_results():
    """ Get Speed Processing Result """
    client = MongoClient(config.MONGO_SERVER)
    db = client.bgl_speed
    
    errors_count_pipeline = [
        {
            '$group': {
                '_id': None,
                'errors_count': {'$sum': '$errors_count'}
            }
        }
    ]

    errors_count = next(db.fatal_error_counts_10_11.aggregate(errors_count_pipeline), 
                        {'errors_count': 0})['errors_count']
    
    
    total_resynch_pipeline = [
        {
            '$group': {
                '_id': '$month',
                'average_resynch_count': {'$avg': '$resynch_count'}
            }
        }
    ]
    total_resynch_result = list(db.total_resynch_counts.aggregate(total_resynch_pipeline))
    
    top5_dates_pipeline = [
        {
            '$group': {
                '_id': '$date',
                'count': {'$max': '$count'}
            }
        },
        {
            '$sort': {'count': -1}
        },
        {
            '$limit': 5
        },
        {
            '$project': {'_id': 0, 'date': '$_id', 'count': 1}
        }
    ]
    top5_dates_result = list(db.top5_dates.aggregate(top5_dates_pipeline))
    
    # smallest_appbusy_node = list(db.smallest_appbusy_node.find({}, {'_id': 0}))[0]
    smallest_appbusy_note_pipeline = [
        {
            '$sort': {'app_busy_num': 1}
        },
        {
            '$limit': 1
        },
        {
            '$project': {'_id': 0, 'node': 1, 'app_busy_num': 1}
        }
    ]
    smallest_appbusy_note_result = list(db.smallest_appbusy_node.aggregate(smallest_appbusy_note_pipeline))
    return errors_count, total_resynch_result, top5_dates_result, smallest_appbusy_note_result


def combine_results(batch_results, speed_results):
    # unpack data
    batch_errors_count, batch_total_resynch_counts, batch_top5_dates, batch_smallest_appbusy_node, batch_earliest_fatal_kernel_date = batch_results
    speed_errors_count, speed_total_resynch_counts, speed_top5_dates, speed_smallest_appbusy_node = speed_results

    # merge total_resynch_counts
    total_resynch_counts = batch_total_resynch_counts

    # merge errors_count
    errors_count = batch_errors_count + speed_errors_count

    # merge smallest_appbusy_node
    if speed_smallest_appbusy_node and (speed_smallest_appbusy_node[0]['app_busy_num'] < batch_smallest_appbusy_node[0]['app_busy_num']):
        smallest_appbusy_node = speed_smallest_appbusy_node
    else:
        smallest_appbusy_node = batch_smallest_appbusy_node

    # merge top5_dates and sort them
    top5_dates = speed_top5_dates + batch_top5_dates
    top5_dates = sorted(top5_dates, key=lambda x: x["count"], reverse=True)[:5]

    # merge earliest_fatal_kernel_date
    earliest_fatal_kernel_date = batch_earliest_fatal_kernel_date

    return errors_count, total_resynch_counts, top5_dates, smallest_appbusy_node, earliest_fatal_kernel_date

