package com.github.fescalhao.movies_project.layers.silver.entities

import com.github.fescalhao.movies_project.aws.s3.S3.readCSV
import com.github.fescalhao.movies_project.generics.ApplicationParams
import com.github.fescalhao.movies_project.layers.traits.{MovieEntity, MovieEntityObject}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

class Ratings(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params) with MovieEntity {
  val logger: Logger = Logger.getLogger(getClass.getName)
  override def execute(): Unit = {
    val path = "s3a://fescalhao-movies/bronze/ratings_small.csv"

    logger.info(s"Reading ${params.entity().capitalize} CSV file")
    val ratingsDF = readCSV(spark, schema, path)

    ratingsDF.show(truncate = false)
  }
}

object Ratings extends MovieEntityObject {
  override def apply(configFilePath: String, params: ApplicationParams): Ratings = {
    val ratings = new Ratings(configFilePath, params)
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
