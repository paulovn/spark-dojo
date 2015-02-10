# Movistar TV logs                                 -*-encoding: utf-8-*-       
#
# Transform the records, specially processing the timestamps
# ----------------------------------------------------------------------------------

from operator import add
import codecs
import sys
import datetime
import time
import random

from pyspark import SparkContext, SparkConf

# Source file
directory = "hdfs:///data/training/"
name = 'tvlogs_tstamp-s0.001'


# ---------------------------------------------------------------------------
# 

def parse_log( record ):
    """
    Parse a Movistar TV log line. Return a list of fields
    """
    # Split by the field separator in the logs
    fields = record.split('|')
    # Find out the timestamp. Generate a version with a 4-hour displacement
    # (so that we give the first 4 night hours to the previous day, i.e. our "day" starts 
    # at actually 4 AM)
    fullts = fields[1] + ' ' + fields[2]
    ts = datetime.datetime.strptime(fullts,'%d/%b/%Y %H:%M:%S') - datetime.timedelta(hours=4)
    # Create 3 new fields: absolute timestamp (Unix time), day (also Unix time) and hour
    # (we know nothing about the time zone; let's asume localtime)
    fepoch = time.mktime(ts.timetuple())
    fday = time.mktime( ts.date().timetuple() )
    ftime  = ts.time()
    ftime = ftime.hour*60 - ftime.minute
    # Return the record
    return [fields[0], ts.isoformat(), fepoch, fday, ftime] + fields[4:]


# ---------------------------------------------------------------------------

# Prepare a Spark configuration. Modify it to allow overwriting output files
conf = SparkConf().setAppName( "Processing Movistar TV logs - {0}".format(sys.argv[0]) )
conf.set( 'spark.hadoop.validateOutputSpecs', False )


# Create a Spark context and set it to work
with SparkContext(conf=conf) as sc:

    # Read the records, and parse them to create a list of fields
    logs = sc.textFile( "{0}{1}.csv".format(directory,name) ).map( parse_log )

    # Keep them around
    logs.cache()

    # Save those fields into an output file, in CSV format
    csv_logs = logs.map( lambda x : ','.join( map(str,x)) )
    csv_logs.saveAsTextFile( "hdfs:///user/{0}/data/tvlogs/fields.csv".format(sc.sparkUser()) )

    # We could also save them serialized, as Python pickle
    logs.saveAsPickleFile( "hdfs:///user/{0}/data/tvlogs/fields.pkl".format(sc.sparkUser()) )
