package com.github.fescalhao.scala_project.movies.layers.silver.transformations

import com.github.fescalhao.scala_project.movies.layers.silver.schemas.MoviesMetadataSchema._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, regexp_replace, to_date}

object MoviesMetadataTransformation {
  def transformMoviesMetadata(df: DataFrame): DataFrame = {
    df
      .withColumn("belongs_to_collection", regexp_replace(col("belongs_to_collection"), "None", "''"))
      .withColumn("belongs_to_collection", from_json(col("belongs_to_collection"), belongsToCollectionSchema))
      .withColumn("genres", from_json(col("genres"), genresSchema))
      .withColumn("production_companies", from_json(col("production_companies"), productionCompaniesSchema))
      .withColumn("production_countries", from_json(col("production_countries"), productionCountriesSchema))
      .withColumn("spoken_languages", from_json(col("spoken_languages"), spokenLanguagesSchema))
      .withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))
  }
}
