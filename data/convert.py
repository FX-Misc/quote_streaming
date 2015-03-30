import time
import sys
import csv
from sas7bdat import SAS7BDAT

with open('rawdata.csv', 'w') as f:
    fwriter = csv.writer(f)
    with SAS7BDAT('ct_20110103.sas7bdat') as inputfile:
        time_start = time.time()
        for row in inputfile:
            fwriter.writerow(row)
            time_end = time.time()
        print "Seconds Elapsed: " + str(time_end - time_start)
