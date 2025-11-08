package com.hakkache.UsingDataFrameDataSet
import org.apache.spark.sql._
import org.apache.log4j._



object DataFramesDataSet {

  case class Person(id:Int,name:String,age:Int,friends:Int)

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val people =spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/fakefriends.csv")
      .as [Person]

    println("Here is our infered Schema")
    people.printSchema()

    println("Let's select the name column : ")
    people.select("name").show()

    println("Filter age under 21 year :")
    people.filter(people("age")<21).show()

    println("group by age :")
    people.groupBy("age")

    println("make everyone 10 years older")
    people.select(people("name"),people("age")+10)

    spark.stop()



  }

}
