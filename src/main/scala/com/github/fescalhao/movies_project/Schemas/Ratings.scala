package com.github.fescalhao.movies_project.Schemas

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

object Ratings {
  def getRatingsSchema: StructType = {
    StructType(List(
      StructField("userId", IntegerType, nullable = false),
      StructField("movieId", IntegerType, nullable = false),
      StructField("rating", DoubleType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
    ))
  }
}
