package com.github.fescalhao.movies_project.layers.silver.entities

import com.github.fescalhao.movies_project.aws.s3.S3.readCSV
import com.github.fescalhao.movies_project.generics.ApplicationParams
import com.github.fescalhao.movies_project.getSparkSession
import com.github.fescalhao.movies_project.layers.traits.{MovieEntity, MovieEntityObject}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

class Credits(params: ApplicationParams) extends MovieEntity {
  override val logger: Logger = Logger.getLogger(getClass.getName)

  override def execute(): Unit = {
    val configFilePath = "src/main/resources/silver/spark.conf"

    logger.info("Initializing Spark Session...")
    val spark: SparkSession = getSparkSession(configFilePath, "movies_silver_credits")

    val path = "data/credits.csv"
    //    val path = "s3a://fescalhao-movies/bronze/credits.csv"

    val ratingsDF = readCSV(spark, schema, path)

    ratingsDF.show(truncate = false)
  }

}

object Credits extends MovieEntityObject {
  override def apply(params: ApplicationParams): Credits = {
    val credits = new Credits(params)
    credits.schema = getSchema
    credits
  }

  override def getSchema: Option[StructType] = {
    Option(
      StructType(List(
        StructField("id", IntegerType, nullable = false),
        StructField("cast", StructType(List(
          StructField("id", IntegerType, nullable = false),
          StructField("cast_id", IntegerType),
          StructField("character", StringType),
          StructField("credit_id", StringType),
          StructField("gender", IntegerType),
          StructField("name", StringType),
          StructField("order", IntegerType),
          StructField("profile_path", StringType)
        ))),
        StructField("crew", StructType(List(
          StructField("id", IntegerType, nullable = false),
          StructField("credit_id", StringType),
          StructField("department", StringType),
          StructField("gender", IntegerType),
          StructField("job", StringType),
          StructField("name", StringType),
          StructField("profile_path", StringType)
        )))
      ))
    )
  }
}
