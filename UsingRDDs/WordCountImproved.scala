package com.hakkache.UsingRDDs
import org.apache.spark._
import org.apache.log4j._

object WordCountImproved {

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","WordCountImproved")

    val input = sc.textFile("data/book.txt")

    val words =  input.flatMap(x=>x.split("\\W+"))

    val lowerCaseWords =words.map(x=>x.toLowerCase())

    val wordsCount = lowerCaseWords.countByValue()

    val sorted = wordsCount.toSeq.sortBy(-_._2)

    sorted.foreach(println)
  }
}