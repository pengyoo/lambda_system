#!/usr/bin/env python
from mrjob.job import MRJob
from mrjob.step import MRStep

# - 1117838572 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.52.041388 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction c
# ache parity error corrected
# Command: python MRJobErrorCounts.py -r hadoop hdfs://localhost:54310/bgl/BGL.log -o hdfs://localhost:54310/bgl/result01

class MRJobTotalResync(MRJob):
    
    """ 5. For each month, what is the average number of seconds over which ”re-synch state events” occurred?  """
    def mapper(self, _, line):
        """ Filter Data and map data """
        try:
            # Preprocess Data and get desired feature
            columns = line.split(" ")
            year = columns[2][:4]
            month = columns[2][5:7]
            message = ' '.join(columns[9:])
            key = year + "-" + month

            # Filter data and return map result
            if 're-synch state' in message:
                yield key, 1

        except Exception as e:
            self.set_status('Error in mapper: {}'.format(e))
    
    def reducer(self, key, values):
        """ Reduce the values """
        try:
            yield key, sum(values)
        except Exception as e:
            self.set_status('Error in reducer: {}'.format(e))

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
        ]


if __name__ == '__main__':
    MRJobTotalResync.run()

