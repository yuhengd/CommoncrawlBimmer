import csv, subprocess


for jobid in range(480,570):
	qsub_command = "qsub -v JOBID="+str(jobid)+" myjobsingle.pbs"
	print qsub_command
	exit_status = subprocess.call(qsub_command, shell=True)
	if exit_status is 1:
		print "Job "+str(jobid)+" failed to sumbit"

print "Done submitting jobs!"
