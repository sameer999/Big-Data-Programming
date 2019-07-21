import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.graphframes._

object icp {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setMaster("local[2]").setAppName("Graph")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Graphs")
      .config(conf =conf)
      .getOrCreate()

    //loading datasets
    val trips_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("D:\\Projects\\ICP5_1\\201508_trip_data.csv")

    val station_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("D:\\Projects\\ICP5_1\\201508_station_data.csv")

    // Printing the Schema
    trips_df.printSchema()
    station_df.printSchema()

    //creating Temp View
    trips_df.createOrReplaceTempView("Trips")
    station_df.createOrReplaceTempView("Stations")

    //printing Dockcount
    station_df.select("dockcount").distinct().show()

    val nstation = spark.sql("select * from Stations")
    val ntrips = spark.sql("select * from Trips")

    //Creating vertices for graph frame
    val stationVertices = nstation
      .withColumnRenamed("name", "id")
      .distinct()

    //Creating edges for graph frame
    val tripEdges = ntrips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")

    //Creating a graph frame with vertices and edges
    val stationGraph = GraphFrame(stationVertices, tripEdges)
    station_df.select(concat(col("lat"),lit(" "),col("long"))).alias("location").show(10)

    tripEdges.cache()
    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + ntrips.count)

    //Printing vertices and edges
    stationGraph.vertices.show()
    stationGraph.edges.show()

    //in degree
    val inDeg = stationGraph.inDegrees
    println("InDegree" + inDeg.orderBy(desc("inDegree")).limit(5))
    inDeg.show(5)

    //out degree
    val outDeg = stationGraph.outDegrees
    println("OutDegree" + outDeg.orderBy(desc("outDegree")).limit(5))
    outDeg.show(5)

    //motif findings: edge between a, b and also edge between b,a
    val motifs = stationGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motifs.show()

    //_______BONUS________
    val ver = stationGraph.degrees
    ver.show(5)
    println("Degree" + ver.orderBy(desc("Degree")).limit(5))

    //saving vertices of graph frame into a csv file
    stationGraph.vertices.write.csv("D:\\Projects\\ICP5_1\\Vertices_dir")

    //saving edges of graph frame into a csv file
    stationGraph.edges.write.csv("D:\\Projects\\ICP5_1\\Edges_dir")

    //finding the common destinations
    val heighestdestination = stationGraph
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    heighestdestination.show(10)

    //calculating indegree to outdegree ratio
    val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
      .drop(outDeg.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")

    degreeRatio.cache()
    //displaying ratio in descending order
    degreeRatio.orderBy(desc("degreeRatio")).show(10)

  }
}