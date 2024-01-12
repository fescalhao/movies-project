package com.github.fescalhao.movies_project.silver.entities

import com.github.fescalhao.movies_project.{MovieEntity, getSparkConf}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

class Ratings(map: Map[String, String]) extends MovieEntity {
  private val logger: Logger = Logger.getLogger(getClass.getName)
  def execute(): Unit = {
    logger.info("Getting Spark Configuration for ...")
    val sparkConf: SparkConf = getSparkConf(
      configFilePath = "src/main/resources/silver/spark.conf",
      appName = "movies_silver_ratings"
    )

    logger.info("Initializing Spark Session...")
    val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val ratingsDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .schema(getSchema)
      .load("s3a://fescalhao-movies/bronze/ratings_small.csv")

    ratingsDF.show(truncate = false)
  }

  def getSchema: StructType = {
    StructType(List(
      StructField("userId", IntegerType, nullable = false),
      StructField("movieId", IntegerType, nullable = false),
      StructField("rating", DoubleType, nullable = true),
      StructField("timestamp", LongType, nullable = true)
    ))
  }
}
