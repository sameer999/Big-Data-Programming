import os
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

os.environ["SPARK_HOME"] = "C:\\spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"

sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
sqlContext = SQLContext(sc)

lines = ssc.socketTextStream("localhost", 23)
words = lines.flatMap(lambda line: line.split(" "))

s1=words.window(10,3)
s2=words.window(10,2)

res=s1.join(s2)
res.pprint()
ssc.start() 
ssc.awaitTermination()