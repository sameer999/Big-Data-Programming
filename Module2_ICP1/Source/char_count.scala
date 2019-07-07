package com.scala.demo

object char_count {

  def main(args: Array[String]) {

    val c: String = "CarDeerbearRiverdark".toLowerCase

    val map = scala.collection.mutable.HashMap.empty[Char, Int]

    for (symbol <- c) {
      if (map.contains(symbol))
        map(symbol) = map(symbol) + 1
      else
        map.+=((symbol, 1))
    }

    println(map)

  }
}
