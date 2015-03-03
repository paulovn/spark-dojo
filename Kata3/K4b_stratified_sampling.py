# Quijote words - stratified sample                                 -*-encoding: utf-8-*-
# ----------------------------------------------------------------------------------

# Stratified sampling
# ===================
# In this script we are going to sample a dataset of words, but we'll sample carefully to 
# avoid removing subsets with low occurrence
# In other words, we'll perform a _stratified sampling_
# 
# In our case we'll use the word length as the feature whose statistics we want to preserve,
# so we will perform an 1% sampling *for each word length*. And we'll change the sampling 
# ratio for the long words (which are typically rare), so as to preserve them.

from __future__ import division  # ensure we're using floating point division

from operator import add
import codecs
import sys
import random
import numpy as np

from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName( "Extracting Quijote words - {0}".format(sys.argv[0]) )
with SparkContext(conf=conf) as sc:

    # Read the file from HDFS into a RDD
    words = sc.textFile( "hdfs:///user/{0}/data/quijote-words.txt".format(sc.sparkUser()) )

    # Create an (k,v) RDD grouping words by length
    wordsByLength = words.map( lambda x : (len(x),x) )

    # Compute the fraction of words having each length
    lengthStats = words.map( lambda x : (len(x),1) ).reduceByKey( add ).sortByKey( False )

    # Get the very long words, out of curiosity
    #print wordsByLength.filter( lambda x : x[0]>16 ).collect()

    # Get those figures back to the driver program, since we need to work with them
    # We convert them to a NumPy array
    stats = np.array( lengthStats.collect() )

    # How many words in total?
    total_full = sum(stats[::,1])

    # And what are the probabilities?
    fraction = stats[::,1]/total_full
    # Stack the fractions into our array. Though we don't actually need it
    stats2 = np.c_[stats,fraction]

    # Ok, as a general rule we want 1% of data for each category
    sample_fractions = np.ones( fraction.shape )*0.01
    # but: underrepresented categories (less than 100 instances) we want at 100%
    sample_fractions[ stats[:,1] < 100 ] = 0.1
    # and very rare categories (less than 20 instances) we want in full
    sample_fractions[ stats[:,1] < 20 ] = 1

    # Construct the dict "key:fraction" for the stratified sampling
    s = dict( zip(map(int,stats[:,0]),sample_fractions) )

    # Sample!
    wordsSampledbyLength = wordsByLength.sampleByKey(False,s)

    # How many did we get?
    total_sampled = wordsSampledbyLength.count()
    fraction = total_sampled/total_full

    # Check amount per category
    lengthStatsSampled = wordsSampledbyLength.mapValues( lambda x : 1 ).reduceByKey( add ).sortByKey( False )
    sampled_stats = lengthStatsSampled.collect()
    # Write results out
    with open( 'sampled-sizes.txt','w') as out:
        print >>out, "# Total sampled: {0}".format(total_sampled)
        print >>out, "# Overall sampling ratio: {0:.4f}".format(fraction)
        
        for (b,a) in zip(stats,sampled_stats):
            print >>out, '{0:2}  {1:6}  {2:.4f}'.format(a[0], a[1], a[1]/b[1])




