package com.hakkache.UsingDataFrameDataSet
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._

object MinTemperaturesDataSet {

  case class Temperature(stationID:String,date:Int,measure_type :String,temperature :Float)

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MinTemperaturesDataSet")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationID",StringType,nullable = true)
      .add("date",IntegerType,nullable=true)
      .add("measure_type",StringType,nullable = true)
      .add("temperature",FloatType,nullable=true)

    import spark.implicits._
    val ds =spark.read
      .schema(temperatureSchema)
      .csv("data/1800.csv")
      .as [Temperature]

    val minTemp = ds.filter($"measure_type"==="TMIN")

    val stationTemp = minTemp.select("stationID","temperature")

    val minTempByStation = stationTemp.groupBy("stationID").min("temperature")

    val minTempByStationF = minTempByStation
      .withColumn("temperature", round($"min(temperature)" * 0.1 * (9.0f / 5.0f) + 32.0f, 2).cast(FloatType))
      .select("stationID","temperature").sort("temperature")

    val results = minTempByStationF.collect()

    for (result<-results){

      val station = result(0)
      val temp =result(1).asInstanceOf[Float]

      val formattedTemp =f"$temp%.2f F"

      println(s"$station minimum temperature : $formattedTemp")

    }


  }



}
