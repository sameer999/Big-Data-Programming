import org.apache.spark._
import org.apache.spark.sql.Row

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

import org.apache.log4j._
import org.apache.spark.sql.functions.countDistinct

object icp3 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","C:\\winutils" )

    val conf = new SparkConf().setMaster("local[2]").setAppName("icp3")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .config(conf =conf)
      .getOrCreate()

    import spark.implicits._

    //Importing the dataset
    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Sameer Yarlagadda\\Downloads\\survey.csv")


    //Saving data to a file

    df.write.format("csv").option("header","true").save("output")


    //Union operation and output is ordered by country name alphabetically

    val df1 = df.limit(7)
    val df2 = df.limit(8)
    println("dataframe 1")
    df1.show()
    println("dataframe 2")
    df2.show()

    val df_union = df1.union(df2)
    println("Union operation and output is ordered by country name alphabetically")
    df_union.orderBy("Country").show()


    df.createOrReplaceTempView("survey")

    // Duplicate
    val df_unique= df.groupBy("Timestamp","Age","Gender","Country","state","self_employed","family_history","treatment","work_interfere","no_employees","remote_work","tech_company","benefits","care_options").count.filter($"count">1).show()
    val df_total=df.count()
    println("duplicate records:"+df_unique)
    println("total records:"+df_total)


    //Groupby Query based on treatment.
    val df_treatment = spark.sql("select count(Country) from survey GROUP BY treatment ")
    println("group by treatment")
    df_treatment.show()


    //Maximum Age
    val df_max = spark.sql("select Max(Age) from survey")
    println("Maximum age")
    df_max.show()

    //Average Age
    val df_avg = spark.sql("select Avg(Age) from survey")
    println("Average age")
    df_avg.show()


    // Join operation
    val df3 = df.limit(50)
    val df4 = df.limit(50)

    df3.createOrReplaceTempView("left")
    df4.createOrReplaceTempView("right")

    val df_join = spark.sql("select left.state,right.Country FROM left,right where left.Age = " +
      "right.Age")
    println("left join and right join")
    df_join.show()


    //13th Row from DataFrame
    val df_13 = df.take(13).last
    println("13th row")
    print(df_13)


    def parseLine(line: String) =
    {
      val fields = line.split(",")
      val timestamp = fields(0).toString
      val age = fields(1).toString
      val noemply = fields(9).toString
      (timestamp,age, noemply)
    }

    val lines = sc.textFile("C:\\Users\\Sameer Yarlagadda\\Downloads\\survey.csv")
    val rdd = lines.map(parseLine).toDF()
    println("")
    println("After parsing")
    rdd.show()
  }
}
