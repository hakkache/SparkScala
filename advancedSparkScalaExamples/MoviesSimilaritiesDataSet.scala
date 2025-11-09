package com.hakkache.advancedSparkScalaExamples
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.sql.{Dataset,SparkSession}
import org.apache.spark.sql.types.{IntegerType,LongType,StringType,StructType}



object MoviesSimilaritiesDataSet {

  case class Movies(userID:Int,movieID:Int,rating:Int,timestamp:Long)
  case class MoviesNames(movieID:Int,movieTitle:String)
  case class MoviePairs(movie1:Int,movie2:Int,rating1:Int,rating2:Int)
  case class MoviePairsSimilarity(movie1:Int,movie2:Int,score:Double,numPairs:Long)

  def computeCosineSimilarity (spark:SparkSession,data:Dataset[MoviePairs]): Dataset[MoviePairsSimilarity]={

    val pairScores =data
      .withColumn("xx",col("rating1")*col("rating1"))
      .withColumn("yy",col("rating2")*col("rating2"))
      .withColumn("xy",col("rating1")*col("rating2"))

    val calculateSimilarity = pairScores
      .groupBy("movie1", "movie2")
      .agg(
        sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx"))) * sqrt(sum(col("yy")))).alias("denominator"),
        count(col("xy")).alias("numPairs")
      )

    import spark.implicits._
    val results = calculateSimilarity
      .withColumn("score",when(col("denominator")=!=0,col("numerator")/col("denominator"))
        .otherwise(null)
      ).select("movie1","movie2","score","numPairs").as[MoviePairsSimilarity]
    results


  }

  def getMovieName(moviesNames: Dataset[MoviesNames],movieID:Int):String ={
    val result =moviesNames.filter(col("movieID")===movieID)
      .select("movieTitle").collect()(0)

    result(0).toString
  }

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("MoviesSimilaritiesDataSet")
      .master("local[*]")
      .getOrCreate()

    val moviesNamesSchema = new StructType()
      .add("movieID",IntegerType,nullable = true)
      .add("movieTitle",StringType,nullable = true)

    val moviesSchema =new StructType()
      .add("UserID",IntegerType,nullable = true)
      .add("movieID",IntegerType,nullable=true)
      .add("rating",IntegerType, nullable=true)
      .add("timestamp",LongType,nullable = true)

    println("\nLoading movie names...")

    import spark.implicits._

    val moviesNames =spark.read
      .option("sep","|")
      .option("charset","ISO-8859-1")
      .schema(moviesNamesSchema )
      .csv("data/ml-100k/u.item")
      .as[MoviesNames]

    val movies = spark.read
      .option("sep","\t")
      .schema(moviesSchema)
      .csv("data/ml-100k/u.data")
      .as [Movies]

    val ratings =movies.select("userID","movieID","rating")

    val moviePairs = ratings.as("ratings1")
      .join(ratings.as("ratings2"),$"ratings1.userID"===$"ratings2.userID" && $"ratings1.movieID"< $"ratings2.movieID")
      .select($"ratings1.movieID".alias("movie1"),$"ratings2.movieID".alias("movie2"),$"ratings1.rating".alias("rating1"),$"ratings2.rating".alias("rating2"))
      .as[MoviePairs]

    val moviePairSimilarities =computeCosineSimilarity(spark,moviePairs).cache()

    if(args.length>0){
      val scoreThreshold =0.97
      val coOccurrenceThreshold =50

      val movieID :Int =args(0).toInt

      val filteredResults = moviePairSimilarities.filter(
        (col("movie1")===movieID||col("movie2")===movieID) && col("score")> scoreThreshold && col("numPairs")> coOccurrenceThreshold)

      val results =filteredResults.sort(col("score").desc).take(10)

      println("\n Top 10 similar movies for " + getMovieName(moviesNames,movieID))

      for (result <- results) {
        var similarMovieID = result.movie1
        if (similarMovieID == movieID) {
          similarMovieID = result.movie2
        }
        println(f"${getMovieName(moviesNames, similarMovieID)}%-50s score: ${result.score}%.2f  strength: ${result.numPairs}")
      }


    }

  }



}
