package com.github.fescalhao.movies_project.layers.silver.entities

import com.github.fescalhao.movies_project.generics.ApplicationParams
import com.github.fescalhao.movies_project.{getSparkSession, logger}
import org.apache.spark.sql.SparkSession

class MasterEntity(configFilePath: String, params: ApplicationParams) {
  logger.info("Initializing Spark Session...")
  val spark: SparkSession = getSparkSession(configFilePath, params.appName)
}
