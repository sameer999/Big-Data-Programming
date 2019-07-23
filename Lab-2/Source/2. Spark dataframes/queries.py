from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os

# Setting environment variables for hadoop and Spark
os.environ["SPARK_HOME"] = "C:\\spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# different struct types
newSchema = StructType([
    StructField("Year", IntegerType(), True),
    StructField("DateTime", TimestampType(), True),
    StructField("Stage", StringType(), True),
    StructField("Stadium", StringType(), True),
    StructField("City", StringType(), True),
    StructField("HomeTeamName", StringType(), True),
    StructField("HomeTeamGoals", IntegerType(), True),
    StructField("AwayTeamGoals", IntegerType(), True),
    StructField("AwayTeamName", StringType(), True),
    StructField("Winconditions", StringType(), True),
    StructField("Attendance", IntegerType(), True),
    StructField("Half-timeHomeGoals", IntegerType(), True),
    StructField("Half-timeAwayGoals", IntegerType(), True),
    StructField("Referee", StringType(), True),
    StructField("Assistant1", StringType(), True),
    StructField("Assistant2", StringType(), True),
    StructField("RoundID", IntegerType(), True),
    StructField("MatchID", IntegerType(), True),
    StructField("HomeTeamInitials", StringType(), True),
    StructField("AwayTeamInitials", StringType(), True)])

# loading dataset
df = spark.read.format("csv").option("header","true").load("WorldCupMatches.csv")
df.show()



# number of teams that reached different stages of the game
df.select(df['HomeTeamName'], df['Stage']).groupBy("Stage").count().show()




# Selecting the Home teams scored goals >=2 and <=5
df.createOrReplaceTempView("table1")
Goals = spark.sql(" SELECT Stage, City, HomeTeamName, HomeTeamGoals FROM table1 \
WHERE HomeTeamGoals >= 3 AND HomeTeamGoals <= 10 ")
Goals.show()

# correlated subquery for finding attendance and max_attendance for a stadium
df.createOrReplaceTempView("tableA")
df.createOrReplaceTempView("tableB")
Max_Attendance = spark.sql("SELECT A.Stage,A.Stadium,A.City,A.HomeTeamName,A.AwayTeamName,A.Attendance,\
(SELECT MAX(Attendance)FROM tableB B where A.Stadium = B.Stadium) max_attendance FROM tableA A ORDER BY max_attendance asc")
Max_Attendance.show()


# Left outer join query
left_outer_join = spark.sql("SELECT A.Stadium,A.City,HomeTeamName,A.AwayTeamName,A.Attendance,B.max_attendance FRom tableA A LEFT OUTER JOIN \
(SELECT Stadium,MAX(Attendance) max_attendance FROM tableB B GROUP BY Stadium) B ON B.Stadium = A.Stadium ORDER BY max_attendance")
left_outer_join.show()


# pattern matching query
Pattern_reg = spark.sql("SELECT * from table1 WHERE Stage LIKE 'Final' AND HomeTeamInitials LIKE 'ITA'")
Pattern_reg.show()


# Average goals scored by a team
Avg_goals = spark.sql("SELECT HomeTeamName, ROUND(AVG(HomeTeamGoals),0) FROM table1 GROUP BY HomeTeamName")
Avg_goals.show()


# total rows after UNION ALL
Union_all = spark.sql("SELECT COUNT(*) AS total_rows FROM (SELECT * FROM tableA UNION ALL SELECT * from tableB)sub")
Union_all.show()


#Full join
join_all = spark.sql("SELECT * FROM tableA FULL JOIN tableB ON tableA.HomeTeamGoals = tableB.AwayTeamGoals")
join_all.show()


# number of times a country scores goals greater than or equal to 6
df.createOrReplaceTempView("table3")
query9 = spark.sql("SELECT HomeTeamName ,COUNT(HomeTeamGoals) FROM table3 where HomeTeamGoals >=6 GROUP BY HomeTeamName ORDER BY 2 DESC")
query9.show()

# number of maximum number of goals scored by a country between years 2000 nd 2010
query9 = spark.sql("SELECT HomeTeamName, MAX(HomeTeamGoals) from table3 where Year BETWEEN 2000 AND 2010 GROUP BY HomeTeamName ORDER BY 2 DESC")
query9.show()

# Total number of goals scored by a country
query10 = spark.sql("SELECT HomeTeamName, SUM(HomeTeamGoals) from table3 GROUP BY HomeTeamName ORDER BY 2 DESC")
query10.show()