import sys
import os
from pyspark import SparkConf, SparkContext

def setupSpark():

    SPARK_HOME = "/usr/local/spark" # Set this to wherever you have compiled Spark
    os.environ["SPARK_HOME"] = SPARK_HOME # Add Spark path
    os.environ["SPARK_LOCAL_IP"] = "10.84.228.133" # Set Local IP
    sys.path.append( SPARK_HOME + "/python") # Add python files to Python Path
    

