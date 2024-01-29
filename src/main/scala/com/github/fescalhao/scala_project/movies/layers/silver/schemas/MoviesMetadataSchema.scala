package com.github.fescalhao.scala_project.movies.layers.silver.schemas

import org.apache.spark.sql.types.{ArrayType, BooleanType, FloatType, IntegerType, StringType, StructField, StructType}

object MoviesMetadataSchema {
  val bronzeSchema: Option[StructType] = Option(
      StructType(List(
        StructField("adult", BooleanType, nullable = false),
        StructField("belongs_to_collection", StringType),
        StructField("budget", IntegerType),
        StructField("genres", StringType),
        StructField("homepage", StringType),
        StructField("id", IntegerType, nullable = false),
        StructField("imdb_id", StringType),
        StructField("original_language", StringType),
        StructField("original_title", StringType),
        StructField("overview", StringType),
        StructField("popularity", StringType),
        StructField("poster_path", StringType),
        StructField("production_companies", StringType),
        StructField("production_countries", StringType),
        StructField("release_date", StringType),
        StructField("revenue", IntegerType),
        StructField("runtime", FloatType),
        StructField("spoken_languages", StringType),
        StructField("status", StringType),
        StructField("tagline", StringType),
        StructField("title", StringType, nullable = false),
        StructField("video", BooleanType),
        StructField("vote_average", FloatType),
        StructField("vote_count", IntegerType)
      ))
    )

  val belongsToCollectionSchema: ArrayType = ArrayType(
    StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("poster_path", StringType, nullable = false),
      StructField("backdrop_path", StringType, nullable = false),
    )), containsNull = true
  )

  val genresSchema: ArrayType = ArrayType(
    StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    )), containsNull = true
  )

  val productionCompaniesSchema: ArrayType = ArrayType(
    StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    )), containsNull = true
  )

  val productionCountriesSchema: ArrayType = ArrayType(
    StructType(List(
      StructField("iso_3166_1", StringType, nullable = false),
      StructField("name", StringType, nullable = false)
    )), containsNull = true
  )

  val spokenLanguagesSchema: ArrayType = ArrayType(
    StructType(List(
      StructField("iso_639_1", StringType, nullable = false),
      StructField("name", StringType, nullable = false)
    )), containsNull = true
  )
}
