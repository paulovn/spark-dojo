# Take a tiny sample from the Movistar TV logs               -*-encoding: utf-8-*-       
# ----------------------------------------------------------------------------------

from operator import add
import codecs
import sys
import random

from pyspark import SparkContext, SparkConf

rate = 0.001
conf = SparkConf().setAppName("Sampling Movistar TV logs via Spark sample - {0}".format(rate) )

# Using the `with' notatton turns the SparcContext into a Python context manager object. 
# A stop() will automatically be done upon leaving the block
with SparkContext(conf=conf) as sc:

    # Read the file from HDFS into a RDD
    #lines = sc.textFile("hdfs:///user/dojocdn/movistarTV/logs/processed/20140801-20140809_no_player/dojocdn_logs.csv")
    directory = "hdfs:///user/dojocdn/movistarTV/logs/processed/20140801-20140809_no_player/"
    name = 'dojocdn_with_timestamp'
    lines = sc.textFile( "{0}{1}.csv".format(directory,name) )

    # Sample the lines
    sampled_lines = lines.sample( False, rate )

    # Save them out
    outname = "hdfs:///data/training/{1}-sample_{2}.csv".format(name,rate)
    sampled_lines.saveAsTextFile( outname )
