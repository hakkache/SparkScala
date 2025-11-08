package com.hakkache.UsingRDDs
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext


object TotalSpentByCustomerSorted {
  def parseLine (line:String) : (String,Float) ={

    val fields =line.split(",")

    (fields(0),fields(2).toFloat)
  }



  def main(args :Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","TotalSpentByCustomer")

    val input =sc.textFile("data/customer-orders.csv")

    val parsedline = input.map(parseLine)

    val SpentByCutsomer = parsedline.reduceByKey((x,y)=>x+y)

    val flipped =SpentByCutsomer.map(x=>(x._2,x._1))

    val sortedSpentByCutsomer = flipped.sortByKey()

    val results = sortedSpentByCutsomer.collect()


    results.foreach(println)

  }

}
