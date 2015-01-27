Here we are going to make the same operation as in Kata 1 (counting
words and sorting by frequency), but we'll create a random sample from
our dataset (so the process will be made with a smaller dataset)

Of course, the sampling operation is so useful that is already a part of the RDD API. 
So we could have done just:

lines = lines.sample( False, 0.5 )        

... but let's do it in our own just to learn.

(note that the Spark code takes care of feeding seeds across the
partitions to better ensure randomness, which we don't do here for the sake
of simplicity).
