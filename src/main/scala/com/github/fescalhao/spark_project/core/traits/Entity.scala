package com.github.fescalhao.spark_project.core.traits

import com.github.fescalhao.spark_project.core.ApplicationParams
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType

trait Entity {
  val logger: Logger

  var schema: Option[StructType] = None

  def execute(): Unit
}

trait EntityObject {
  def apply(configFilePath: String, params: ApplicationParams): Entity
}


