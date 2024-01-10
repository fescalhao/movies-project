package com.github.fescalhao.movies_project

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import Schemas.Ratings._

object Main extends Serializable with App {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  logger.info("Getting Spark Configuration...")
  val sparkConf = getSparkConf("movies")

  logger.info("Initializing Spark Session...")
  val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ratingsDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .schema(getRatingsSchema)
    .load("s3a://fescalhao-movies/bronze/ratings_small.csv")

  ratingsDF.show(truncate = false)
}


