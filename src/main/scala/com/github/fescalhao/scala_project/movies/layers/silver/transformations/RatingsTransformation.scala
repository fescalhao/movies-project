package com.github.fescalhao.scala_project.movies.layers.silver.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_unixtime}

object RatingsTransformation {
  def transformRatings(df: DataFrame): DataFrame = {
      df.withColumn("timestamp", from_unixtime(col("timestamp"), "yyyy-MM-dd HH:mm:ss z"))
  }
}
