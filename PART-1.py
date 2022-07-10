import os, sys,datetime

from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions  import lag, cos, sin, lit, toRadians, atan2, sqrt, sum
from pyspark.sql.window import Window


no_of_sec_per_day= 24*3600


cores= os.cpu_count()


def sparkEnvConfig():
    util_path= "C:/winutil"

    util_path = os.path.abspath(util_path)

    python_path= 'python'

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
    return SparkSession.builder.master(f"local[{cores}]").config(conf=conf).appName("Big Data Analysis").getOrCreate()

sparkEnvConfig()
spark=createSparkSession()

def readDataInDf(file):
    return spark.read.csv(file, header=True)

df = readDataInDf('example.csv')




columns=[]
for col in df.dtypes:
    columns.append(col[0])


    

#question 1
addition= datetime.timedelta(hours=8)

def increase_time_by_8_hours(row):
    result={}

    for col in columns:
        result[col]=row[col]

    ts= row.Date.split('-') + row.Time.split(':')
    
    time_stamp= datetime.datetime(int(ts[0]),int(ts[1]),int(ts[2]),int(ts[3]),int(ts[4]),int(ts[5]))
    
    beijing_time= time_stamp+ addition

    result['Date']=beijing_time.strftime("%Y-%m-%d")
    result['Time']= beijing_time.strftime("%H:%M:%S")

    date_1 = '30/12/1899 00:00:00'
    
    date_format_str = '%d/%m/%Y %H:%M:%S'
    start = datetime.datetime.strptime(date_1, date_format_str)
    diff = beijing_time - start
  
    diff_in_days = diff.total_seconds()/no_of_sec_per_day
   
    result['Timestamp']=diff_in_days
    return result   




rdd2=df.rdd.map(lambda x: increase_time_by_8_hours(x))
 
df2=rdd2.toDF()
df2.show(5)



df2.createOrReplaceTempView("df_view")


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

print("The first 5 User IDs and their longest difference in time stamps ")
df5.show(5)




# question 6

w = Window().partitionBy("UserID").orderBy("Timestamp")

def dist(long_x, lat_x, long_y, lat_y): 
    R=lit(6371.0)
    lat1 = toRadians(lat_x)
    lon1 = toRadians(long_x)
    lat2 = toRadians(lat_y)
    lon2 = toRadians(long_y)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance
df6= df2.withColumn("Distance", dist(
    "Longitude", "Latitude",
    lag("Longitude", 1).over(w), lag("Latitude", 1).over(w)
).alias("Distance"))


df6.createOrReplaceTempView("df_view")


df7= spark.sql(
    """
      WITH distance_per_day (UserID,Date, distance)  AS 
                            (SELECT UserID,Date,SUM(Distance) AS distance
                            FROM df_view
                            GROUP BY UserID,Date)        
      SELECT distance_per_day.UserID,MIN(distance_per_day.Date) AS earliest_date ,longest_distances.max_distance AS longest_distance
      FROM distance_per_day,( SELECT UserID,MAX(distance) AS max_distance
                    FROM distance_per_day
                    GROUP BY UserID) AS longest_distances
      WHERE longest_distances.UserID= distance_per_day.UserID AND distance_per_day.distance=longest_distances.max_distance
      GROUP BY distance_per_day.UserID,longest_distances.max_distance
    """
)



print("Users and the earliest day theys they travelled the most. ")
df7.show(df7.count(), False)
total_distance = df6.groupBy().sum().collect()[0][1]
print(total_distance)


