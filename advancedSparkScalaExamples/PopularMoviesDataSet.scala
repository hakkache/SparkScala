package com.hakkache.advancedSparkScalaExamples
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}


object PopularMoviesDataSet {

  final case class Movie(movie_id: Int)
  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark =SparkSession
      .builder
      .appName("PopularMoviesDataSet")
      .master("local[*]")
      .getOrCreate()

    val moviesSchema = new StructType()
      .add("user_id",IntegerType,nullable = true)
      .add("movie_id",IntegerType,nullable=true)
      .add("rating",IntegerType,nullable=true)
      .add("timestamp",LongType,nullable = true)

    import spark.implicits._

    val moviesDS =spark.read
      .option("sep","\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as [Movie]

    val topMoviesID = moviesDS.groupBy("movie_id").count().orderBy(desc("count"))

    topMoviesID.show(10)

    spark.stop()


  }

}
