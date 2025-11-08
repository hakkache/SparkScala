package com.hakkache.UsingDataFrameDataSet
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._




object FriendsByAgeDataset {


  case class FakeFriends(id:Int,name: String, age:Int,friends :Long)

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark =SparkSession
      .builder
      .appName("FriendsByAge")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ds =spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/fakefriends.csv")
      .as [FakeFriends]

    val friendsByAge = ds.select("age","friends")

    friendsByAge.groupBy("age").avg("friends").show()

    friendsByAge.groupBy("age").avg("friends").sort("age").show()


    friendsByAge.groupBy("age").agg(round(avg("friends"), 2))
      .sort("age").show()

    friendsByAge.groupBy("age").agg(round(avg("friends"), 2))
      .sort("age").sort("age").show()






  }
}
