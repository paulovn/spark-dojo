# Movistar TV logs                                 -*-encoding: utf-8-*-       
# Compute a histogram of intervals
# ----------------------------------------------------------------------------------


from operator import add
import codecs
import sys
import datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row


# Source file
name = 'intervals.txt'

# Prepare a Spark configuration. Modify it to allow overwriting output files
conf = SparkConf().setAppName( "Processing Movistar TV logs - {0}".format(0) )
conf.set( 'spark.hadoop.validateOutputSpecs', False )

# Create a Spark context and set it to work
with SparkContext(conf=conf) as sc:

    # Read the intervals. We'll need them to be float for the histogram computation to work
    directory = "hdfs:///user/{0}/data/tvlogs/".format( sc.sparkUser() )
    intervals = sc.textFile( "{0}{1}".format(directory,name) ).map( lambda x : float(x) )
    intervals.cache()

    # Compute a histogram
    bins, values = intervals.histogram( 40 )

    # The histogram is a couple of regular Python lists, received by the driver. 
    # Let's write them out in a nice form
    bins = map( lambda x :datetime.timedelta(minutes=x), bins )
    with open( 'intervals-histogram.txt', 'w' ) as out:
        print >>out, "Histogram"
        for i in range(len(values)):
            print >>out, "[{0!s:20}, {1!s:20}) : {2}".format( bins[i], bins[i+1], values[i] )


    # Another histogram, this time we specify the bins
    hbins = [0, 5, 10, 15, 20, 25, 30,
             60*24, 60*24*2, 60*24*3, 60*24*4, 60*24*5, 60*24*6, 60*24*7, 60*24*8, 60*24*9 ]
    bins2, values2 = intervals.histogram( hbins )

    bins2 = map( lambda x :datetime.timedelta(minutes=x), bins2 )
    with open( 'intervals-histogram.txt', 'a' ) as out:
        print >>out, "\nHistogram 2"
        for i in range(len(values2)):
            print >>out, "[{0!s:20}, {1!s:20}) : {2}".format( bins2[i], bins2[i+1], values2[i] )
