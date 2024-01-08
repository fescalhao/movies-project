package com.github.fescalhao.movies_project

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Main extends Serializable with App {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  logger.info("Getting Spark Configuration...")
  val sparkConf = getSparkConf("movies")

  logger.info("Initializing Spark Session...")
  val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val df = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/movies_metadata.csv")

  df.show(truncate = false)
}
