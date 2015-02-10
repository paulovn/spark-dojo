# Quijote words - take 2b                                 -*-encoding: utf-8-*-
# ----------------------------------------------------------------------------------

from operator import add
import codecs
import sys

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName( "Quijote words - 2b" )
sc = SparkContext(conf=conf)

# Read the file from HDFS into a RDD
lines = sc.textFile("hdfs:///data/training/quijote.utf8.txt")


# Create a new RDD with all the words in the file (splitting by whitespace in each record)
words = lines.flatMap( lambda x: x.strip().split(None) )

# Create a translation table to remove punctuation marks
removing = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~¡¿'
translate_table = dict( (ord(char),None) for char in removing )

# Turn the words RDD into a (key,value) RDD, in which the key is the word and value is 1
# In the process, remove also punctuation marks
words_kv = words.map( lambda x: (x.translate(translate_table), 1) )

# Reduce by adding the values for all records with the same key
counts = words_kv.reduceByKey( add )

# Collect results
output = counts.collect()

# Output stats to a local file
dest = codecs.open( 'quijote-words-2b.txt', 'w', encoding='utf-8' )
for (word, count) in output:
    print >>dest, u"{0:15} : {1}".format(word, count)

sc.stop()
