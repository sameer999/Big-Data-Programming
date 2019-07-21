import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.graphframes._

object icp{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Graph Algorithms")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Graph Algorithms")
      .config(conf =conf)
      .getOrCreate()

    //Loading datasets
    val trips_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("201508_trip_data.csv")

    val station_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("201508_station_data.csv")


    //First of all create three Temp View

    trips_df.createOrReplaceTempView("Trips")
    station_df.createOrReplaceTempView("Stations")


    val nstation = spark.sql("select * from Stations")
    val ntrips = spark.sql("select * from Trips")

    //vertices
    val stationVertices = nstation
      .withColumnRenamed("name", "id")
      .distinct()

    //Edges
    val tripEdges = ntrips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")


    //Creating graph frame
    val stationGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()

      // Triangle Count
    val stationTraingleCount = stationGraph.triangleCount.run()
    stationTraingleCount.select("id","count").show()


    // Shortest Path
    val shortPath = stationGraph.shortestPaths.landmarks(Seq("Japantown","Santa Clara County Civic Center")).run
    shortPath.show()


    //Page Rank
    val stationPageRank = stationGraph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.show()
    stationPageRank.edges.show()


    //Saving to File
    stationGraph.vertices.write.csv("D:\\Projects\\icp6\\vertices")
    stationGraph.edges.write.csv("D:\\Projects\\icp6\\edges")


    //Label propogation
    val lpa = stationGraph.labelPropagation.maxIter(5).run()
    lpa.select("id", "label").show()


    // BFS
    val pathBFS = stationGraph.bfs.fromExpr("id = 'Japantown'").toExpr("dockcount < 15").run()
    pathBFS.show()
    println("BFS ")

  }

}
