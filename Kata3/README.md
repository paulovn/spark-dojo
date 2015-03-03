We will now improve the sampling process by using a more refined
version, able to do stratified sampling. I.e., we segment our datasets
into clusters and sample each class separately, so as to keep the
ration of items across clusters. Or, alternatively, use different
sampling rations for each class (e.g. if we want to boost
representativeness of some classes).

We'll prepare first a version of the dataset with all the words
already extracted, and write it to HDFS to have it ready for repeated
use. Then we perform the sampling. There are two versions included:
 * A standard PySpark script. This can be submitted as usual
 * An IPython Notebook version, that can be loaded and executed in a web-based Notebook connected to the cluster.
