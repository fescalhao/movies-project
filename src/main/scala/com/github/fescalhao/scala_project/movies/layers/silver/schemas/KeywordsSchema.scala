package com.github.fescalhao.scala_project.movies.layers.silver.schemas

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object KeywordsSchema {

  val bronzeSchema: Option[StructType] = Option(
      StructType(List(
        StructField("id", IntegerType, nullable = false),
        StructField("keywords", StructType(List(
          StructField("id", IntegerType, nullable = false),
          StructField("name", StringType, nullable = false),
        )), nullable = false)
      ))
    )
}
