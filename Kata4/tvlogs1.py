# Movistar TV logs                                 -*-encoding: utf-8-*-       
# Characterize the logs by extracting some basic data
# ----------------------------------------------------------------------------------


from operator import add, itemgetter
import codecs
import sys
import datetime
import time
import random

from pyspark import SparkContext, SparkConf
from pyspark.mllib.stat import Statistics

# Data source file, in HDFS's user area. It should be a comma-separated CSV file
name = 'fields.csv'


# ----------------------------------------------------------------------------------

def minmax(a, b):
    """
    Given two (min,max) pairs, return the extrema of both
    """
    return min(a[0], b[0]), max(a[1], b[1])


# ----------------------------------------------------------------------------------

# Prepare a Spark configuration. Modify it to allow overwriting output files
conf = SparkConf().setAppName( "Processing Movistar TV logs - {0}".format(0) )
conf.set( 'spark.hadoop.validateOutputSpecs', False )

# Create a Spark context and set it to work
with SparkContext(conf=conf) as sc:

    # Read the parsed records, split fields (producing a list of strings)
    directory = "hdfs:///user/{0}/data/tvlogs/".format( sc.sparkUser() )
    logs = sc.textFile( "{0}{1}".format(directory,name) ).map( lambda x: x.split(',') )

    # Keep them around
    logs.cache()

    # Write out some stats to a local file
    with open( 'stats.txt', 'w' ) as out:

        # (A) Count the number of events
        print >>out, "* Num events", logs.count()

        # (B) Count the number of distinct users
        users = logs.map( lambda x : x[0] ).distinct()
        print >>out, "* Num users", users.count()

        # (C) Count the number of distinct items
        items = logs.map( lambda x : x[6] ).distinct()
        print >>out, "* Num items", items.count()

        # (D) Find out the minimum and maximum date in the logs
        # We use the timestamp field. Since it will be a string, we need to cast it to float
        # We duplicate it since we need to reduce both for min and max
        timestamps = logs.map( lambda x : (float(x[2]), float(x[2])) )
        # Reduce it by applying the defined minmax function
        tsmin, tsmax = timestamps.reduce(minmax)
        print >>out, "* Date interval: ",
        # Print out the dates, as ISO 8601 strings
        for ts in tsmin, tsmax:
            print >>out, datetime.datetime.fromtimestamp(ts).isoformat(),
        print >>out, "\n",

        # (E) Find the top 100 users by number of events
        # First let's create an RDD of (<user>,<num events>)
        user_events = logs.map( lambda x : (x[0],1) ).reduceByKey( add )
        #print >>out, user_events.take(30)
        # Now find the top 100 records in that RDD
        top_users = user_events.top( 100, key=itemgetter(1) )
        print >>out, "\n* Top users"
        for u in top_users:
            print >>out, '{1:8} : {0}'.format( *u )

        # (F) Find the top 100 items by number of events
        # This time we use the countByKey() action, which does not produce an RDD, but a
        # plain Python dictionary returned to the driver
        item_events = logs.map( lambda x : (x[6],) ).countByKey()
        #print >>out, item_events
        # Since it's a dictionary, we sort it at the driver side, by using standard Python gear
        top_items = sorted( item_events, key=item_events.get, reverse=True )
        print >>out, "\n* Top items"
        for u in top_items[:100]:
            print >>out, '{0:8} : {1}'.format( item_events[u], u )


