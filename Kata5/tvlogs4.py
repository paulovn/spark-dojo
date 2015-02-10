# Movistar TV logs                                 -*-encoding: utf-8-*-       
# Do some queries
# ----------------------------------------------------------------------------------


from operator import add
import codecs
import sys
import datetime
import time
import random

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

# Source file
name = 'fields.csv'



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

    # Read the parsed records
    directory = "hdfs:///user/{0}/data/tvlogs/".format( sc.sparkUser() )
    logs = sc.textFile( "{0}{1}".format(directory,name) ).map( lambda x: x.split(',') )


    sqlContext = SQLContext(sc)
    logdata = logs.map( lambda p: Row( user=p[0], date=p[1], type=p[4], content=p[5]) )


    # Infer the schema, and register the SchemaRDD as a table.
    # SQL can be run over SchemaRDDs that have been registered as a table.
    db = sqlContext.inferSchema( logdata )
    db.registerTempTable( "logs" )

    # Find out who connected on Aug 8th
    data = sqlContext.sql("SELECT DISTINCT(user) FROM logs WHERE date > '2014-08-01' AND date < '2014-08-02' ORDER BY user" )
    # The results of SQL queries are RDDs and support all the normal RDD operations.

    # Write those users to disk
    with open( 'users.txt', 'w' ) as out:
        for row in data.collect():
            print >>out, row.user
        
    # And save them to disk
    #query.saveAsTextFile( "hdfs:///user/{0}/data/tvlogs/users.csv".format( sc.sparkUser()) )

    
