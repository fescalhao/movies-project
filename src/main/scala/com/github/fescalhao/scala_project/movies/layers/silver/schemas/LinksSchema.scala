package com.github.fescalhao.scala_project.movies.layers.silver.schemas

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object LinksSchema {
  val bronzeSchema: Option[StructType] = Option(
      StructType(List(
        StructField("movieId", IntegerType, nullable = false),
        StructField("imdbId", IntegerType, nullable = true),
        StructField("tmdbId", IntegerType, nullable = false)
      ))
    )
}
