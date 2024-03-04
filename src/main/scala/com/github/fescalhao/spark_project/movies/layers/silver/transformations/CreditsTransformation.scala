package com.github.fescalhao.spark_project.movies.layers.silver.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, lit, regexp_replace}
import com.github.fescalhao.spark_project.movies.layers.silver.schemas.CreditsSchema._

object CreditsTransformation {

  def transformCredits(df: DataFrame): DataFrame = {
    df
      .withColumn("cast", regexp_replace(col("cast"), "None", "''"))
      .withColumn("crew", regexp_replace(col("crew"), "None", "''"))
      .withColumn("cast", from_json(col("cast"), castSchema))
      .withColumn("crew", from_json(col("crew"), crewSchema))
  }
}
