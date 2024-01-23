package com.github.fescalhao.movies_project.layers.traits

import com.github.fescalhao.movies_project.generics.ApplicationParams
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType

trait MovieEntity {
  val logger: Logger

  var schema: Option[StructType] = None
  def execute(): Unit
}

trait MovieEntityObject {
  def apply(params: ApplicationParams): MovieEntity

  def getSchema: Option[StructType] = None
}


