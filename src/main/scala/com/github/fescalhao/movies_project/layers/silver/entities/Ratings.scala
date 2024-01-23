package com.github.fescalhao.movies_project.layers.silver.entities

import com.github.fescalhao.movies_project.aws.s3.S3.readCSV
import com.github.fescalhao.movies_project.generics.ApplicationParams
import com.github.fescalhao.movies_project.getSparkSession
import com.github.fescalhao.movies_project.layers.traits.{MovieEntity, MovieEntityObject}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import scala.io.Source

class Ratings(params: ApplicationParams) extends MovieEntity {
  override val logger: Logger = Logger.getLogger(getClass.getName)

  override def execute(): Unit = {
    val configFilePath = "silver/spark.conf"

    logger.info("Initializing Spark Session...")
    val spark: SparkSession = getSparkSession(configFilePath, "movies_silver_ratings")

    val path = "s3a://fescalhao-movies/bronze/ratings_small.csv"

    val ratingsDF = readCSV(spark, schema, path)

    ratingsDF.show(truncate = false)
  }
}

object Ratings extends MovieEntityObject {
  override def apply(params: ApplicationParams): Ratings = {
    val ratings = new Ratings(params)
    ratings.schema = getSchema
    ratings
  }

  override def getSchema: Option[StructType] = {
    Option(
      StructType(List(
        StructField("userId", IntegerType, nullable = false),
        StructField("movieId", IntegerType, nullable = false),
        StructField("rating", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      ))
    )
  }
}
