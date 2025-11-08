package com.hakkache.UsingRDDs
import org.apache.spark._
import org.apache.log4j._

object FriendsByAge {

  def parseLine (line:String) : (Int,Int) ={

    val fields =line.split(",")

    val age = fields(2).toInt
    val numFriends = fields(3).toInt

    (age,numFriends)
  }


  def main(args:Array[String]) : Unit= {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","FriendsByAge ")

    val lines = sc.textFile("data/fakefriends.csv")

    val cleanedLines = lines.filter(line => ! line.contains("age"))

    val rdd = cleanedLines.map(parseLine)

    val totalByAge = rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))

    val averagesByAge = totalByAge.mapValues(x=> x._1/x._2)

    val results = averagesByAge.collect()

    results.sorted.foreach(println)


  }

}