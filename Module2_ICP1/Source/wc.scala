package com.scala.demo
import org.apache.spark.{SparkContext, SparkConf}

object wc {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc=new SparkContext(sparkConf)

    val input =  sc.textFile("C:\\Users\\Sameer Yarlagadda\\IdeaProjects\\1\\input.txt")

    val words = input.flatMap(line => line.split("\\W+"))
    println("after splitting:")
    words.foreach(f=>println(f))

    val counts = words.map(words => (words, 1)).reduceByKey(_+_,1)
    println("after mapping:")
    counts.foreach(f=>println(f))

    val wordsList=counts.sortBy(outputLIst=>outputLIst._1,ascending = true)
    println("after sorting:")
    wordsList.foreach(outputLIst=>println(outputLIst))

    wordsList.saveAsTextFile("wc_output")

    wordsList.take(10).foreach(outputLIst=>println(outputLIst))
    println("Total no of unique words:",wordsList.count())

    sc.stop()

  }

}