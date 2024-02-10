package com.github.fescalhao.scala_project.core.aws.s3

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, SparkSession}

case class BucketBy(numBuckets: Int, colName: String, colNames: String*)

object S3 {
  def readCSV(
               spark: SparkSession,
               schema: Option[StructType],
               path: String,
               options: Map[String, String]
             ): DataFrame = {
    val pathList: List[String] = path :: Nil
    readCSV_(spark, schema, pathList, options)
  }

  def readCSV(
               spark: SparkSession,
               schema: Option[StructType],
               pathList: List[String],
               options: Map[String, String]
             ): DataFrame = {
    readCSV_(spark, schema, pathList, options)
  }

  private def readCSV_(
                        spark: SparkSession,
                        schema: Option[StructType],
                        pathList: List[String],
                        options: Map[String, String]
                      ): DataFrame = {

    val dfr: DataFrameReader = spark.read.format("csv")

    options.foreach(op => dfr.option(op._1, op._2))

    schema match {
      case Some(v) => dfr.schema(v)
      case None => dfr.option("inferSchema", "true")
    }

    dfr.load(pathList : _*)
  }

  def writeDelta(
                  df: DataFrame,
                  targetPath: String,
                  mode: String = "overwrite",
                  partitionBy: Option[List[String]] = None,
                  sortBy: Option[List[String]] = None,
                  options: Map[String, String] = Map()
                ): Unit = {
    val dfw = df.write
      .format("delta")
      .mode(mode)

    partitionDataframe(dfw, partitionBy)
    sortDataframe(dfw, sortBy)

    options.foreach(op => dfw.option(op._1, op._2))

    dfw.save(targetPath)
  }

  def writeDeltaAsTable(
                         df: DataFrame,
                         tableName: String,
                         mode: String = "overwrite",
                         partitionBy: Option[List[String]] = None,
                         sortBy: Option[List[String]] = None,
                         bucketBy: Option[BucketBy] = None,
                         options: Map[String, String] = Map()
                       ): Unit = {
    val dfw = df.write
      .format("delta")
      .mode(mode)

    partitionDataframe(dfw, partitionBy)
    sortDataframe(dfw, sortBy)
    bucketDataframe(dfw, bucketBy)

    options.foreach(op => dfw.option(op._1, op._2))

    dfw.saveAsTable(tableName)
  }

  private def partitionDataframe[T](dfw: DataFrameWriter[T], partitionColumns: Option[List[String]]): DataFrameWriter[T] = {
    partitionColumns match {
      case Some(p) => dfw.partitionBy(p: _*)
      case None => dfw
    }
  }

  private def sortDataframe[T](dfw: DataFrameWriter[T], sortColumns: Option[List[String]]): DataFrameWriter[T] = {
    sortColumns match {
      case Some(s) =>
        if (s.length > 1) {
          dfw.sortBy(s.head, s.tail: _*)
        } else {
          dfw.sortBy(s.head)
        }
      case None => dfw
    }
  }

  private def bucketDataframe[T](dfw: DataFrameWriter[T], bucketValues: Option[BucketBy]): DataFrameWriter[T] = {
    bucketValues match {
      case Some(b) => dfw.bucketBy(b.numBuckets, b.colName, b.colNames: _*).sortBy(b.colName, b.colNames: _*)
      case None => dfw
    }
  }
}
