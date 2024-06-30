#!/usr/bin/env python
import datetime
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRJobErrorCounts(MRJob):
    """ 1. How many fatal log entries in the months of October or November resulted from a 'major internal error'?  """

    
    def mapper(self, _, line):
        """ Filter Data and map data """
        try:
            # Preprocess Data and get desired feature
            columns = line.split(" ")
            month = columns[2][5:7]
            level = columns[8]
            timestamp = columns[4]
            message = ' '.join(columns[9:])

            # Filter data and return map result
            if month in ["10", "11"] and level == 'FATAL' and 'major internal error' in message:
                yield None, 1

        except Exception as e:
            self.set_status('Error in mapper: {}'.format(e))

    def combiner(self, _, values):
        """ User combiner to improve the performance  """
        yield None, sum(values)    
    
    def reducer(self, _, values):
        """ Sum the values """
        try:
            yield "errors_count", sum(values)
        except Exception as e:
            self.set_status('Error in reducer: {}'.format(e))

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
        ]


if __name__ == '__main__':
    MRJobErrorCounts.run()

