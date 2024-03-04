package com.github.fescalhao.spark_project.movies.layers.silver.schemas

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

object RatingsSchema {

  val bronzeSchema: Option[StructType] = Option(
      StructType(List(
        StructField("userId", IntegerType, nullable = false),
        StructField("movieId", IntegerType, nullable = false),
        StructField("rating", DoubleType, nullable = true),
        StructField("timestamp", LongType, nullable = true)
      ))
    )
}
