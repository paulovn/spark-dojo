# Quijote words                                          -*-encoding: utf-8-*-
# Write to HDFS a file with all the words extracted from Quijote
# ----------------------------------------------------------------------------------

from operator import add
import codecs
import sys
import random

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName( "Extract Quijote words" )
with SparkContext(conf=conf) as sc:

    # Read the file from HDFS into a RDD
    lines = sc.textFile("hdfs:///data/training/quijote.utf8.txt")

    # Create a new RDD with all the words in the file (by splitting the lines in our RDD)
    words = lines.flatMap( lambda x: x.strip().split(None) )

    # Remove punctuation marks
    removing = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~¡¿'
    translate_table = dict( (ord(char),None) for char in removing )
    words = words.map( lambda x: x.translate(translate_table) )

    words.saveAsTextFile( "hdfs:///user/{0}/data/quijote-words.txt".format(sc.sparkUser()) )


