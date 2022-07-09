import os, sys

from pyspark.sql import SparkSession
import pyspark


def sparkEnvConfig():
    #downlaod zipped file from https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip ,extract it 
    #and paste the path that leads to  bin/winutils.exe in util_path. This applies only if one is running on windows os.
    util_path= "C:/winutil"

    util_path = os.path.abspath(util_path)

    #there are some errors that arise since the standard code is calling python3 which through for some systems
    #.The code should just call python. The python variable is set for the environment variable to allow for smooth running.
    python_path= 'python'

    #this is the path where the java 8 jdk is installed.
    java_path= "C:\Program Files\Java\jdk1.8.0_202"

    java_path = os.path.abspath(java_path)

    os.environ['HADOOP_HOME'] = util_path
    os.environ['JAVA_HOME']= java_path
    os.environ['PYSPARK_PYTHON']= python_path
    sys.path.append(util_path)
    sys.path.append(java_path)
    sys.path.append(python_path)
    
def createSparkSession():
    conf = pyspark.SparkConf().set("spark.executor.instances", 4)
    return SparkSession.builder.master(f"local[{no_of_cores}]").config(conf=conf).appName("Big Data Analysis").getOrCreate()

