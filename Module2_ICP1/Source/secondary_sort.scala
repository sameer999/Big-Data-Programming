package com.scala.demo
import org.apache.spark.{SparkConf, SparkContext}

object secondary_sort {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils");
    val conf = new SparkConf().setAppName("secondary_sort").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val output = "secondary_sort_output"

    val personRDD = sc.textFile("C:\\Users\\Sameer Yarlagadda\\IdeaProjects\\1\\secondary_sort_input.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => ((k(0),k(1)), (k(2),k(3))) }
    println("pairsRDD")
    pairsRDD.foreach {
      println
    }
    val numReducers = 2;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))
    println("listRDD")
    listRDD.foreach {
      println
    }

    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }

    resultRDD.saveAsTextFile(output)
    sc.stop()

  }

}