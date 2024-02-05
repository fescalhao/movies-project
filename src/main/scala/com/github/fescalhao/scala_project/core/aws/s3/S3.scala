package com.github.fescalhao.scala_project.core.aws.s3

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, SparkSession}

case class BucketBy(numBuckets: Int, colName: String, colNames: String*)

sealed trait RichStringType[T]

object RichStringType {
  implicit val listInstance: RichStringType[List[String]] =
    new RichStringType[List[String]] {}
  implicit val strInstance: RichStringType[String] =
    new RichStringType[String] {}
  implicit val nilInstance: RichStringType[Nil.type] =
    new RichStringType[Nil.type] {}
}

object S3 {
  def readCSV[A: RichStringType](
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

  def writeDelta[P: RichStringType](
                                    df: DataFrame,
                                    targetPath: String,
                                    mode: String = "overwrite",
                                    partitionBy: P = Nil,
                                    sortBy: P = Nil,
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

  def writeDeltaAsTable[P: RichStringType](
                         df: DataFrame,
                         tableName: String,
                         mode: String = "overwrite",
                         partitionBy: P = Nil,
                         sortBy: P = Nil,
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

  private def loadFiles[L: RichStringType](dfr: DataFrameReader, paths: L): DataFrame = {
    paths match {
      case p: String => dfr.load(p)
      case p: List[String] => dfr.load(p: _*)
    }
  }

  private def partitionDataframe[P: RichStringType, T](dfw: DataFrameWriter[T], partitionColumns: P): DataFrameWriter[T] = {
    partitionColumns match {
      case p: String => dfw.partitionBy(p)
      case p: List[String] => dfw.partitionBy(p: _*)
    }
  }

  private def sortDataframe[P: RichStringType, T](dfw: DataFrameWriter[T], sortColumns: P): DataFrameWriter[T] = {
    sortColumns match {
      case s: String => dfw.sortBy(s)
      case s: List[String] =>
        if (s.length > 1) {
          dfw.sortBy(s.head, s.tail: _*)
        } else {
          dfw.sortBy(s.head)
        }
    }
  }

  private def bucketDataframe[T](dfw:DataFrameWriter[T], bucketValues: Option[BucketBy]): DataFrameWriter[T] = {
    bucketValues match {
      case Some(b) => dfw.bucketBy(b.numBuckets, b.colName, b.colNames: _*).sortBy(b.colName, b.colNames: _*)
    }
  }
}
