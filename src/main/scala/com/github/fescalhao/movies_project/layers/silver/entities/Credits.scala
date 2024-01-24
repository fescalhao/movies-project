package com.github.fescalhao.movies_project.layers.silver.entities

import com.github.fescalhao.movies_project.aws.s3.S3.readCSV
import com.github.fescalhao.movies_project.generics.ApplicationParams
import com.github.fescalhao.movies_project.layers.traits.{MovieEntity, MovieEntityObject}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class Credits(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params) with MovieEntity {
  override def execute(): Unit = {
    val path = "s3a://fescalhao-movies/bronze/credits.csv"

    val creditsDF = readCSV(spark, schema, path)

    creditsDF.show(truncate = false)
  }

}

object Credits extends MovieEntityObject {
  override def apply(configFilePath: String, params: ApplicationParams): Credits = {
    val credits = new Credits(configFilePath, params)
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
