
This Kata changes dataset: we are now processing a batch of [Movistar TV logs][1]

There are different versions of these logs. We will work with the version with this structure:

   `83.42.94.87|31/Jul/2014|23:40:37|23|V|_32497/SPA_PR_SD_EU_169__19fd7ab16a4f4cd620140620122845.ism`

... that is, each line is a record with fields separated by a pipe. The fields are:

    user IP
    date, as day/month/year
    time (probably local time, though it is not clear)
    hour (truncated from time)
    content type (L=live, V=vod)
    content id

The first thing we'll do is create a tiny sample, so that the exercises run fast.


[1]: http://wikis.hi.inet/kg/index.php/ML:TVLogAnalysis
