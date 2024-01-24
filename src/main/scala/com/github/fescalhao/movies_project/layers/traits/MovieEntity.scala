package com.github.fescalhao.movies_project.layers.traits

import com.github.fescalhao.movies_project.generics.ApplicationParams
import org.apache.spark.sql.types.StructType

trait MovieEntity {
  var schema: Option[StructType] = None

  def execute(): Unit
}

trait MovieEntityObject {
  def apply(configFilePath: String, params: ApplicationParams): MovieEntity

  def getSchema: Option[StructType] = None
}


