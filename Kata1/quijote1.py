# Quijote words - take 1                                 -*-encoding: utf-8-*-
# ----------------------------------------------------------------------------------

from operator import add
import codecs
import sys

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName( "Quijote words - 1" )
sc = SparkContext(conf=conf)

# Read the file from HDFS into a RDD
lines = sc.textFile("hdfs:///data/training/quijote.utf8.txt")

# Create a new RDD with all the words in the file (by splitting whitespace in our RDD)
words = lines.flatMap( lambda x: x.strip().split(None) )

# Turn the words RDD into a (key,value) RDD, in which the key is the word and value is 1
words_kv = words.map( lambda x: (x, 1) )

# Reduce by adding the values for all records with the same key
counts = words_kv.reduceByKey( add )

# Collect results
output = counts.collect()

# Output stats to a local file
dest = codecs.open( 'quijote-words-1.txt', 'w', encoding='utf-8' )
for (word, count) in output:
    print >>dest, u"[{0}] : {1}".format(word, count)

sc.stop()
