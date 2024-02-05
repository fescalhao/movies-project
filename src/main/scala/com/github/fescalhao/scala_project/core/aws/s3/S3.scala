package com.github.fescalhao.scala_project.core.aws.s3

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriterV2, SparkSession}

object S3 {
  def readCSV[A: StringOrList](
                                spark: SparkSession,
                                schema: Option[StructType],
                                paths: A,
                                options: Map[String, String] = Map()
                              ): DataFrame = {

    val dfr = spark.read.format("csv")

    options.foreach(op => dfr.option(op._1, op._2))

    schema match {
      case Some(v) => dfr.schema(v)
      case None => dfr.option("inferSchema", "true")
    }

    loadFiles(dfr, paths)
  }

  def writeParquet(
                    df: DataFrame,
                    mode: String,
                    targetPath: String,
                    options: Map[String, String] = Map()
                  ): Unit = {
    val dfw = df.write
      .format("parquet")
      .mode(mode)

    options.foreach(op => dfw.option(op._1, op._2))

    dfw.save(targetPath)
  }

  private def loadFiles[B: StringOrList](dfr: DataFrameReader, paths: B): DataFrame = {
    paths match {
      case p: String => dfr.load(p)
      case p: List[String] => dfr.load(p: _*)
    }
  }
}

sealed trait StringOrList[T]

object StringOrList {
  implicit val listInstance: StringOrList[List[String]] =
    new StringOrList[List[String]] {}
  implicit val strInstance: StringOrList[String] =
    new StringOrList[String] {}
}

