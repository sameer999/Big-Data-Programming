import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._
import org.apache.spark.sql.functions._

object pagerank {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val conf = new SparkConf().setMaster("local[1]").setAppName("PAGE RANK")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("PAGE RANK")
      .config(conf =conf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("wordgame.csv")


    // Printing the Schema
    df.printSchema()

    // Vertices
    val v= df.select("word1").toDF("id").distinct()

    // Edges
    val e= df.select("word1","word2").toDF("src","dst").distinct()

    // Graphframe
    val graph = GraphFrame(v, e)

    // printing vertices and edges
    graph.vertices.show()
    graph.edges.show()

    println("Total Number of vertices: " + graph.vertices.count)
    println("Total Number of edges: " + graph.edges.count)

    // Page rank
    val stationPageRank = graph.pageRank.resetProbability(0.15).maxIter(1).run()
    stationPageRank.vertices.select("id","pagerank").sort(desc("pagerank")).show()

  }

}