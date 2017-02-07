# CommoncrawlBimmer
python script and PBS script to download and preprocess user forum data in Commoncrawl

Steps to run the pipeline:

1. submit submitJobarray.py using python. It will use a for loop to submit a bunch of pbs jobs, each time passing one argument $JOBID to the pbs script.

2. In submitJobarray.py, its for loop calls myjobsingle.pbs at each iteration, submitting one pbs job using $JOBID as input, and requesting 4gb memory and 2 cores each time. 

3. In myjobsingle.pbs, it runs the commoncrawlTimestampJobarray.py at each palmetto node, getting timestamps of one specific thread of bimmerpost and one index from commoncrawl. The thread name and index are calculated in the commoncrawlTimestampJobarray.py using the $JOBID passed from previous steps. 
