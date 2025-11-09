package com.hakkache.UsingDataFrameDataSet
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType,DoubleType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._

object TotalSpentByCustomerDataSet {

  case class CustOrders (cust_id:Int,item_id:Int,amount_spent :Double)

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark =SparkSession
      .builder()
      .appName("TotalSpentByCustomerDataSet")
      .master("local[*]")
      .getOrCreate()

    val CustomerOrdersSchema =new StructType()
      .add("cust_id",IntegerType,nullable = true)
      .add("item_id",IntegerType,nullable = true)
      .add("amount_spent",DoubleType,nullable = true)

    import spark.implicits._
    val CustomerDS =spark.read
      .schema(CustomerOrdersSchema)
      .csv("data/customer-orders.csv")
      .as [CustOrders]

    val totalByCustomer =CustomerDS
      .groupBy("cust_id")
      .agg(round(sum("amount_spent"),2).alias("total_spent"))


    val totalByCustomerSorted = totalByCustomer.sort("total_spent")

    totalByCustomerSorted.show(totalByCustomerSorted.count.toInt)

    spark.stop()




  }

}
