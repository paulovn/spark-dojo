# Movistar TV logs                                 -*-encoding: utf-8-*-       
#
# Compute time intervals between consecutive records
# ----------------------------------------------------------------------------------


from operator import add
import codecs
import sys
import datetime
import time
import random

from pyspark import SparkContext, SparkConf
from pyspark.mllib.stat import Statistics

# Data source file, in HDFS's user area. This time we'll load the serialized pickle records
name = 'fields.pkl'


def time_intervals( datapoints ):
    """
    Compute the time differences between log entries by the same user
    """
    # Get the timestamps from the stream, and sort by them
    datapoints = sorted( [t[2] for t in datapoints] )
    first = datapoints[0]
    # Iterate over the rest, computing the difference between consecutive timestamps
    d = list()
    for t in datapoints[1:]:
        d.append( [ (t - first)/60 ]  )    # in minutes
        #d.append( [ int((t - first)/60)+0.5]  )    # in minutes
        first = t
    return d


# Prepare a Spark configuration. Modify it to allow overwriting output files
conf = SparkConf().setAppName( "Processing Movistar TV logs - {0}".format(0) )
conf.set( 'spark.hadoop.validateOutputSpecs', False )


# Create a Spark context and set it to work
with SparkContext(conf=conf) as sc:

    # Read the parsed records. This time we are reading the serialized file. 
    # So in each record the fields will already be split
    directory = "hdfs:///user/{0}/data/tvlogs/".format( sc.sparkUser() )
    logs = sc.pickleFile( "{0}{1}".format(directory,name) )

    # Group records by user
    byUser = logs.map( lambda x : (x[0],x[1:]) ).groupByKey()

    # Compute the time difference between consecutive records of each user
    intervals = byUser.flatMap( lambda (x,y) : time_intervals(y) )

    # keep it for reusing
    intervals.cache()
    
    # Extract statistics from those time differences
    # Note that colStats needs a Vector (or a Python list), since it computes by column
    # In our case we have a 1-column list
    summary = Statistics.colStats(intervals)
    with open( 'interval-stats.txt', 'w' ) as out:
        for s in ('count','mean','variance','min','max','numNonzeros'):
            print >>out, ' * {0}: {1}'.format( s, getattr(summary,s)() )

    # And also save them to disk. Flat the list for that
    flat = intervals.map( lambda x: x[0] )
    flat.saveAsTextFile( "hdfs:///user/{0}/data/tvlogs/intervals.txt".format(sc.sparkUser()) )
