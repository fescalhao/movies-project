package com.github.fescalhao.scala_project.aws.s3

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object S3 {
  def readCSV(spark: SparkSession, schema: Option[StructType], paths: String*): DataFrame = {
    val df = spark.read
      .format("csv")
      .option("header", "true")

    schema match {
      case Some(v) => df.schema(v)
      case None => df.option("inferSchema", "true")
    }

    df.load(paths: _*)
  }
}
