package es.tid.ML

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object CountWords {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CountWords")
    val sc = new SparkContext(conf)

    // text file in HDFS
    val textFile = "hdfs:///user/samson/test/quijote.utf8.txt"

    // Read the file from HDFS into a RDD                                                            
    val lines = sc.textFile(textFile)

    // Create a new RDD with all the words in the file (by splitting the lines in our RDD)
    val words = lines.flatMap(line => line.trim.split(" "))

    // Removing punctuation marks
    // We will use a regular expression
    val puntuationMarks = "[¡!\"\'#%,-.:;?¿{}]"
    val filteredWords = words.map(word => word.replaceAll(puntuationMarks, ""))

    // Turn the words RDD into a (key,value) RDD, in which the key is the word and value is 1
    val wordsKeyValue = filteredWords.map(word => (word, 1))

    // Reduce by adding the values for all records with the same key
    val counts = wordsKeyValue.reduceByKey( _ + _ )

    // Format output string 
    val formattedOutput = counts.map{ case (word, count) => f"[${word}] : ${count}" }

    // Save output to local file
    val outputFile = "quijote-words-2.txt"
    formattedOutput.saveAsTextFile(outputFile)
  }
}

