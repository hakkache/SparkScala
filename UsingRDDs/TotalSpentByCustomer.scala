package com.hakkache.UsingRDDs
import org.apache.spark._
import org.apache.log4j._

object TotalSpentByCustomer {


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

    val results = SpentByCutsomer.collect()


    results.foreach(println)




  }


}