package spark.code
import org.apache.spark.{SparkConf, SparkContext}

object Bfs_Dfs {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setAppName("Bfs_Dfs").setMaster("local[*]")
    val sc = new SparkContext(conf)
    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
    val g: Graph = Map(1 -> List(2,3,4), 2 -> List(1,3,5), 3 -> List(1,2), 4 -> List(2,6),5 -> List(1,6,7),6 -> List(1,2,5,7),7 -> List(1,2,4,5,6))

    def BFS(start: Vertex, g: Graph): List[List[Vertex]] = {

      def BFS0(elems: List[Vertex],visited: List[List[Vertex]]): List[List[Vertex]] = {
        val newNeighbors = elems.flatMap(g(_)).filterNot(visited.flatten.contains).distinct
        if (newNeighbors.isEmpty)
          visited
        else
          BFS0(newNeighbors, newNeighbors :: visited)
      }
      BFS0(List(start),List(List(start))).reverse
    }

    def DFS(start: Vertex, g: Graph): List[Vertex] = {

      def DFS0(v: Vertex, visited: List[Vertex]): List[Vertex] = {
        if (visited.contains(v))
          visited
        else {
          val neighbours:List[Vertex] = g(v) filterNot visited.contains
          neighbours.foldLeft(v :: visited)((b,a) => DFS0(a,b))
        }
      }
      DFS0(start,List()).reverse
    }

    val bfsresult1=BFS(1,g)
    val bfsresult5=BFS(5,g)

    val dfsresult1=DFS(1,g )
    val dfsresult6=DFS(6,g )

    println("Bfs start node: 1",bfsresult1.mkString(","))
    println("Bfs start node: 5",bfsresult5.mkString(","))

    println("Dfs start node: 1",dfsresult1.mkString(","))
    println("Dfs start node: 6",dfsresult6.mkString(","))

  }
}
