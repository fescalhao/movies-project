package com.github.fescalhao.scala_project.movies.layers.silver.schemas

import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object CreditsSchema {

  val bronzeSchema: Option[StructType] = Option(
    StructType(List(
      StructField("cast", StringType),
      StructField("crew", StringType),
      StructField("id", IntegerType)
    ))
  )

  val castSchema: ArrayType = ArrayType(
    StructType(List(
      StructField("cast_id", IntegerType),
      StructField("character", StringType),
      StructField("credit_id", StringType),
      StructField("gender", IntegerType),
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("order", IntegerType),
      StructField("profile_path", StringType)
    )), containsNull = true
  )

  val crewSchema: ArrayType = ArrayType(
    StructType(List(
      StructField("credit_id", StringType),
      StructField("department", StringType),
      StructField("gender", IntegerType),
      StructField("id", IntegerType, nullable = false),
      StructField("job", StringType),
      StructField("name", StringType),
      StructField("profile_path", StringType)
    )), containsNull = true
  )

}
