package com.github.fescalhao.movies_project.silver.entities

import com.github.fescalhao.movies_project.getSparkConf
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

class RatingsImpl(schema: StructType) {
  private val logger: Logger = Logger.getLogger(getClass.getName)

  logger.info("Getting Spark Configuration for ...")
  val sparkConf: SparkConf = getSparkConf(
    configFilePath = "src/main/resources/spark.conf",
    appName = "movies_silver_ratings"
  )

  logger.info("Initializing Spark Session...")
  val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  private val ratingsDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .schema(schema)
    .load("s3a://fescalhao-movies/bronze/ratings_small.csv")

  ratingsDF.show(truncate = false)

}

object Ratings {
  def apply: RatingsImpl = new RatingsImpl(Ratings.ratingsSchema)
  private def ratingsSchema: StructType = {
    StructType(List(
      StructField("userId", IntegerType, nullable = false),
      StructField("movieId", IntegerType, nullable = false),
      StructField("rating", DoubleType, nullable = true),
      StructField("timestamp", LongType, nullable = true)
    ))
  }
}

