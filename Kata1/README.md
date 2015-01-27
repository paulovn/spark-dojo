The examples in this Kata use a small dataset: a file containing the
text from Don Quijote, as downloaded from Project Gutemberg. The file
has already been uploaded to the HDFS file system in our local
cluster, so it can be accessed as

    	     hdfs:///data/training/quijote.utf8.txt

In case it gets deleted, uploading it again is easy:

   	   wget http://www.gutenberg.org/ebooks/2000/...
	   hadoop hdfs -put ... /data/training/quijote.utf8.txt

Notes:
 * The text is in UTF-8, so beware of charsets when processing it
 * On the copy uploaded to HDFS the preamble and end credits inserted
   by Project Gutemberg (announcements, disclaimers, copyrights, etc)
   has been removed, so only the book text remains. Therefore if you
   re-upload from Project Gutemberg without removing those parts, the
   results will be slightly different.

