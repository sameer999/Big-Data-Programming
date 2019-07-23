import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.twitter._
import org.apache.spark.{ SparkContext, SparkConf }

object twitter{

  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val conf = new SparkConf().setMaster("local[3]").setAppName("Twitter_streaming")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {

    sc.setLogLevel("WARN")

    // Twitter stream API credentials are given as arguments
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // setting properties for twitter credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey )
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // creates a new stream for every 10 seconds
    val ssc = new StreamingContext(sc, Seconds(10))

    // Creating the data stream
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Filtering the stream data
    val text = stream.filter(_.getLang()=="en").map(a => a.getText)

    // calculating word count using mapreduce
    val words =text.flatMap(_.split(" ")).map(a => (a,1)).reduceByKey(_+_)

    // saving the output to a file
    words.saveAsTextFiles("wc_output")

    ssc.start()
    ssc.awaitTermination()

  }

}
