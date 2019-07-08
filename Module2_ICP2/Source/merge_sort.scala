package spark.code

import org.apache.spark._

object merge_sort {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    //Spark Context
    val conf = new SparkConf().setAppName("merge_sort").setMaster("local");
    val sc = new SparkContext(conf);

    val a = Array(19, 2, 1, 16, 8, 5, 7, 11);
    val b = sc.parallelize(a);

    val maparray = b.map(x => (x, 1))

    //val sorted = maparray.sortByKey();
    //sorted.keys.collect().foreach(println)


    val n = a.length;
    val l = 0;
    val r = n-1;

    print("\nBefore sorting\n")
    for ( x <- a)
    {
      print(x+"\t");
    }

    sort(a,l,r);

    print("\nAfter sorting\n")
    for ( y <- a)
    {
      print(y+"\t")
    }

    // Merge sort
    def sort(arr: Array[Int], l: Int, r: Int): Unit = {
      if (l < r) {
        // Finding middle
        val m = (l + r) / 2
        // Sorting first half
        sort(arr, l, m)
        // Sorting second half
        sort(arr, m + 1, r)
        // Merging the both sorted halves
        merge(arr, l, m, r)
      }
    }

    // Merge for Array
    def merge(arr: Array[Int], l: Int, m: Int, r: Int): Unit = {

      // Finding sizes of two subarrays to be merged
      val n1 = m - l + 1
      val n2 = r - m

      // Creating temp arrays
      val L = new Array[Int](n1)
      val R = new Array[Int](n2)

      //Copying data to temp arrays
      var a = 0
      while (a < n1){
        L(a) = arr(l + a);
        a += 1;
      }

      var b = 0
      while (b < n2){
        R(b) = arr(m + 1 + b);
        b += 1;
      }

      // Merge the temp arrays
      // Initial indexes of first and second subarrays
      var i = 0
      var j = 0
      // Initial index of merged subarry array
      var k = l
      while (i < n1 && j < n2) {
        if (L(i) <= R(j)) {
          arr(k) = L(i)
          i += 1
        }
        else {
          arr(k) = R(j)
          j += 1
        }
        k += 1
      }

      // Copy remaining elements of L[] if any
      while (i < n1)
      {
        arr(k) = L(i)
        i += 1
        k += 1
      }

      // Copy remaining elements of R[] if any
      while (j < n2)
      {
        arr(k) = R(j)
        j += 1
        k += 1
      }

    }

  }

}