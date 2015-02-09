To get acquainted with Spark processing modes, in this Kata we will
execute one simple Spark example: SparkPi, a Scala app that computes
some digits for PI.

This application is contained in the Spark examples jar. In the CDH5
installation on CentOS, this is located in:

   `/usr/lib/spark/lib/spark-examples-1.2.0-cdh5.3.0-hadoop2.5.0-cdh5.3.0.jar`

and the class to be executed inside the jar is

   `org.apache.spark.examples.SparkPi`


If you want to take a look at the application code, it's in [SparkPi] [1]:

  [1]: <https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala>

We'll use `spark-submit` to execute the application in the Spark
cluster. Follow the instructions in the wiki page to use the different
Spark processing modes.
