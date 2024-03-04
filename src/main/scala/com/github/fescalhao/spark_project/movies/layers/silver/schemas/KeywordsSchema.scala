package com.github.fescalhao.spark_project.movies.layers.silver.schemas

import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object KeywordsSchema {

  val bronzeSchema: Option[StructType] = Option(
      StructType(List(
        StructField("id", IntegerType, nullable = false),
        StructField("keywords", StringType, nullable = false)
      ))
    )

  val keywordsSchema: ArrayType = ArrayType(
    StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
    )), containsNull = true)
}
