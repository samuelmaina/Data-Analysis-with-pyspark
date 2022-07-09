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

sparkEnvConfig()
spark=createSparkSession()

def readDataInDf(file):
    return spark.read.csv(file, header=True)

df = readDataInDf('example.csv')




columns=[]
for col in df.dtypes:
    columns.append(col[0])


    

#question 1

#the duration that will be added to the different dates.
#done globally to save on time since doing it in the increase_time_by_8_hours function
#takes time calling  timedelta function.
addition= datetime.timedelta(hours=8)

def increase_time_by_8_hours(row):
    #object that will be return by the function. This will be used to create a new dataframe.
    result={}

    #copy the prexisting into result
    for col in columns:
        result[col]=row[col]

    #the timestamp that contains the year, month, day , hour, minutes and seconds each contain in the six indices
    #of the ts array.
    ts= row.Date.split('-') + row.Time.split(':')
    
    #the parsed time_stamp which a datetime object that can be manipulated as time in python.
    time_stamp= datetime.datetime(int(ts[0]),int(ts[1]),int(ts[2]),int(ts[3]),int(ts[4]),int(ts[5]))
    
    #the beijing_time
    beijing_time= time_stamp+ addition

    #add the Date and time to the result.
    result['Date']=beijing_time.strftime("%Y-%m-%d")
    result['Time']= beijing_time.strftime("%H:%M:%S")

    #starting time for the time stamp.
    date_1 = '30/12/1899 00:00:00'
    
    date_format_str = '%d/%m/%Y %H:%M:%S'
    start = datetime.datetime.strptime(date_1, date_format_str)
    # Get the interval between two datetimes as timedelta object
    diff = beijing_time - start
  
    #results are supposed to be stored as number of days(with their fractional parts). The total_seconds return
    #a float hence the division will be float division hence no trancation yielding to fractional parts.
    diff_in_days = diff.total_seconds()/no_of_sec_per_day
   
    #store the results of the time stamp in the result
    result['Timestamp']=diff_in_days
    return result   




#create a RDD that is going to be transformed by our lambda function above. The map function can not be 
#called directly on the dataframe.
rdd2=df.rdd.map(lambda x: increase_time_by_8_hours(x))
 
#convert the RDD to Dataframe
df2=rdd2.toDF()
df2.show(5)



# # question 2
# #create a temporary view for sql querying. I have choosen to use sql queries to perform querying.
df2.createOrReplaceTempView("df_view")


# group the data by UserID and order the Data according to the number of each rows each userID has.
# the query should only return only the UserID, and the their day count.
df3= spark.sql(
    """
    SELECT UserID, COUNT(DISTINCT Date) AS Days
    FROM df_view
    GROUP BY UserID
    ORDER BY Days DESC
    """
)

print("User IDs and Days for which data was recorded")
df3.show(5)


df4= spark.sql(
    """
    SELECT UserID, COUNT(*) AS Day_with_more_than_100_data_points
    FROM (SELECT UserID
          FROM df_view
          GROUP BY UserID, Date
          HAVING COUNT(*)>100)
    GROUP BY UserID
    """
)

print("User_ID and the days they had over 100 data points");
#display all the rows
df4.show(df4.count(), False)

# question 5
df5= spark.sql(
    """
    SELECT UserID, MAX(Timestamp) - MIN(Timestamp) AS diff
    FROM df_view
    GROUP BY UserID 
    ORDER BY diff DESC
    """
)

print(" The first 5 User IDs and their longest difference in time stamps ")
df5.show(5)


