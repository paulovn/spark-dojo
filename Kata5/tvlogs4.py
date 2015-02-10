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


# Prepare a Spark configuration. Modify it to allow overwriting output files
conf = SparkConf().setAppName( "Processing Movistar TV logs - {0}".format(sys.argv[0]) )
conf.set( 'spark.hadoop.validateOutputSpecs', False )


# Create a Spark context and set it to work
with SparkContext(conf=conf) as sc:

    # Read the parsed records
    directory = "hdfs:///user/{0}/data/tvlogs/".format( sc.sparkUser() )
    logs = sc.textFile( "{0}{1}".format(directory,name) ).map( lambda x: x.split(',') )

    # Turn each record into a Row object
    logRows = logs.map( lambda p: Row( user=p[0], date=p[1], type=p[4], content=p[5]) )

    # Create an SQL context
    sqlContext = SQLContext(sc)

    # Infer the schema. It will do so by looking at the first 100 rows
    logSchema = sqlContext.inferSchema( logRows )

    # Register the SchemaRDD as a table.
    logSchema.registerTempTable( "logs" )
    # We can now perform SQL queries over the "logs" table

    # An example query: find out who connected on Aug 8th, when they
    # first connected, and how many events we've got on that day for each one of those users
    data = sqlContext.sql("""
       SELECT user, min(date) AS start, count(*) as num 
          FROM logs 
          WHERE date > '2014-08-01' AND date < '2014-08-02' 
          GROUP by user ORDER BY start""" )

    # The results of SQL queries are RDDs and support all the normal RDD operations.

    # Write out the result to disk
    with open( 'users.txt', 'w' ) as out:
        for row in data.collect():
            print >>out, '{0:15} {1} {2:5}'.format( row.user, row.start, row.num )

    
