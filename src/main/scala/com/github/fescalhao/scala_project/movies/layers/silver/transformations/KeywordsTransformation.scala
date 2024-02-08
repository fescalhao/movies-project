package com.github.fescalhao.scala_project.movies.layers.silver.transformations

import com.github.fescalhao.scala_project.movies.layers.silver.schemas.KeywordsSchema.keywordsSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}

object KeywordsTransformation {
  def transformKeywords(df: DataFrame): DataFrame = {
    df
      .withColumn("keywords", from_json(col("keywords"), keywordsSchema))
  }
}
