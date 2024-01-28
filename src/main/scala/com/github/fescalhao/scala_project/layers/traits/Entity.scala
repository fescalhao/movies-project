package com.github.fescalhao.scala_project.layers.traits

import com.github.fescalhao.scala_project.generics.ApplicationParams
import org.apache.spark.sql.types.StructType

trait Entity {
  var schema: Option[StructType] = None

  def execute(): Unit
}

trait EntityObject {
  def apply(configFilePath: String, params: ApplicationParams): Entity

  def getSchema: Option[StructType] = None
}


