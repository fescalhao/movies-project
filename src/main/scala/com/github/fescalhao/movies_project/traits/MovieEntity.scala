package com.github.fescalhao.movies_project.traits

import com.github.fescalhao.movies_project.aws.s3.S3
import org.apache.spark.sql.types.StructType

trait MovieEntity {
  def execute(): Unit

  def getSchema: StructType
}
