package com.hakkache.UsingRDDs
import org.apache.spark._
import org.apache.log4j._

object WordCountImprovedV2 {

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local","WordCountImprovedV2")

    val input = sc.textFile("data/book.txt")

    val words =  input.flatMap(x=>x.split("\\W+"))

    val lowerCaseWords =words.map(x=>x.toLowerCase())

    val wordCounts = lowerCaseWords.map(x=>(x,1)).reduceByKey((x,y)=>x+y)

    val wordCountSorted =wordCounts.map(x=>(x._2,x._1)).sortByKey(ascending = false)

    for (result<- wordCountSorted) {

      val count =result._1
      val word = result._2

      println(s"$word : $count")
    }




  }
}