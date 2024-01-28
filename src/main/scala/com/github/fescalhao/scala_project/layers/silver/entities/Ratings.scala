package com.github.fescalhao.scala_project.layers.silver.entities

import com.github.fescalhao.scala_project.aws.s3.S3.readCSV
import com.github.fescalhao.scala_project.generics.ApplicationParams
import com.github.fescalhao.scala_project.layers.traits.{Entity, EntityObject}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

class Ratings(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params) with Entity {
  val logger: Logger = Logger.getLogger(getClass.getName)
  def execute(): Unit = {

    val sourcePath = s"s3a://fescalhao-${params.project()}/bronze/ratings_small.csv"
    val targetPath = s"s3a://fescalhao-${params.project()}/silver/ratings_small"

    logger.info(s"Reading ${params.entity().capitalize} CSV file")
    val ratingsDF = readCSV(spark, schema, sourcePath)

    ratingsDF.show(truncate = false)

    ratingsDF.write
      .format("delta")
      .mode("overwrite")
      .save(targetPath)
  }
}

object Ratings extends EntityObject {
  def apply(configFilePath: String, params: ApplicationParams): Ratings = {
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
