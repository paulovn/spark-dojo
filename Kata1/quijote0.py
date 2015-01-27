from pyspark import SparkContext, SparkConf

# Create a Spark context
conf = SparkConf().setAppName( "Quijote count - 0" )
sc = SparkContext(conf=conf)

# Create an RDD by reading a text file from HDFS and splitting its lines
quijote = sc.textFile("hdfs:///data/training/quijote.utf8.txt")

# See how many records our RDD has (i.e. number of lines in the read file)
print quijote.count()
