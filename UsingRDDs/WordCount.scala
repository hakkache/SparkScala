package com.hakkache.UsingRDDs
import org.apache.spark._
import org.apache.log4j._

object WordCount {

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","WordCount")

    val input = sc.textFile("data/book.txt")

    val words =  input.flatMap(x=>x.split(" "))

    val wordsCount = words.countByValue()

    val sorted = wordsCount.toSeq.sortBy(-_._2)

    sorted.foreach(println)
  }
}