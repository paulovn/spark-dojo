# Quijote words - take 3                                 -*-encoding: utf-8-*-
# ----------------------------------------------------------------------------------

from operator import add
import codecs
import sys

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName( "Quijote words - 3" )
sc = SparkContext(conf=conf)

# Read the file from HDFS into a RDD
lines = sc.textFile("hdfs:///user/samson/test/quijote.utf8.txt")


# Create a new RDD with all the words in the file (by splitting the lines in our RDD)
words = lines.flatMap( lambda x: x.strip().split(None) )


removing = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~¡¿'
translate_table = dict( (ord(char),None) for char in removing )

# Turn the words RDD into a (key,value) RDD, in which the key is the word and value is 1
# In the process, remove punctuation marks
words_kv = words.map( lambda x: (x.translate(translate_table), 1) )

# Reduce by adding the values for all records with the same key
counts = words_kv.reduceByKey( add )

# And sort by frequency
ordered_counts = counts.map( lambda (a,b) : (b,a) ).sortByKey( False ) 

# Collect results
output = ordered_counts.collect()

# Output stats to a local file
dest = codecs.open( 'quijote-words-3.txt', 'w', encoding='utf-8' )
for (word, count) in output:
    print >>dest, u"{1:15} : {0}".format(word, count)

sc.stop()
