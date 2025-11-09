package com.hakkache.UsingDataFrameDataSet
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._




object WordCountBetterSortedDataSet {

  case class Book(value:String)

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark =SparkSession
      .builder
      .appName("WordCountBetterSortedDataSet")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val input =spark.read.text("data/book.txt").as [Book]
    val words = input
      .select(explode(split($"value", "\\W+")).alias("word"))
      .filter($"word" =!= "")

    val lowerCaseWords =words.select(lower($"word").alias("word"))

    val wordsCounts = lowerCaseWords.groupBy("word").count()

    val wordsCountsSorted =wordsCounts.sort("count")

    wordsCountsSorted.show(wordsCountsSorted.count.toInt)


//    Using RDD and DataSet
    val bookRDD = spark.sparkContext.textFile("data/book.txt")

    val wordsRDD = bookRDD.flatMap(x=>x.split("\\W+"))
    val wordsDS  = wordsRDD.toDS()

    val lowerCaseWordsDS =wordsDS.select(lower($"value").alias("word"))

    val wordCountsDS =lowerCaseWords.groupBy("word").count()

    val wordCountsDSSorted = wordCountsDS.sort("count")

    wordCountsDSSorted.show(wordCountsDSSorted.count().toInt)

    spark.stop()



  }

}
