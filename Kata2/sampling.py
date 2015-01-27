# Quijote words - sampled version                                 -*-encoding: utf-8-*-       
# ----------------------------------------------------------------------------------

from operator import add
import codecs
import sys
import random

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName( "Quijote sampled words" )
sc = SparkContext(conf=conf)

# Read the file from HDFS into a RDD
lines = sc.textFile("hdfs:///user/samson/test/quijote.utf8.txt")



# ---------------------------------------------------------------------------
# Sample our RDD. We define a function (actually a class) and pass it to a mapper

class Sampler( object ):
    """The object provivding the sampling function"""

    def __init__( self, fraction ):
        """Initialize the object"""
        self.fraction = fraction

    def __call__( self, index, collection ):
        """Sample a partition"""
        for obj in collection:
            if random.random() < self.fraction:
                yield obj


# So we sample here:
lines = lines.mapPartitionsWithIndex( Sampler(0.5), True )

# ----------------------------------------------------------------------------

# Create a new RDD with all the words in the file (by splitting the lines in our RDD)
words = lines.flatMap( lambda x: x.strip().split(None) )

removing = u'!"#%\'()*+,-./:;<=>?@[\]^_`{|}~¿¡'
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
dest = codecs.open( 'quijote-sampled-words.txt', 'w', encoding='utf-8' )
for (word, count) in output:
    print >>dest, u"{1:15} : {0}".format(word, count)

sc.stop()

