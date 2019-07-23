import org.apache.spark._
import org.apache.log4j.{Level, Logger}

object Fb_friends{

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    //Controlling log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("Fb_friends").setMaster("local[*]");
    val sc = new SparkContext(conf)

    // Map function
    def Map(line: String) = {
      val words = line.split(" ")
      val key = words(0)
      val pairs = words.slice(1, words.size).map(friend => {
        if (key < friend) (key, friend) else (friend, key)
      })
      pairs.map(pair => (pair, words.slice(1, words.size).toSet))
    }

    // Reduce function
    def Reduce(accumulator: Set[String], set: Set[String]) = {
      accumulator intersect set
    }

    // Input file
    val file = sc.textFile("facebook_combined.txt")

    // final result
    val results = file.flatMap(Map)
      .reduceByKey(Reduce)
      .filter(!_._2.isEmpty)
      .sortByKey()

    results.collect.foreach(line => {
      println(s"${line._1} ${line._2.mkString(" ")}")})

    // saving output to a file
    results.coalesce(1).saveAsTextFile("Fb_friends_output")

      }

}