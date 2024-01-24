package com.github.fescalhao.movies_project.layers.gold

import com.github.fescalhao.movies_project.generics.ApplicationParams
import com.github.fescalhao.movies_project.layers.traits.{LayerEntity, MovieEntityObject}
import com.github.fescalhao.movies_project.{getSparkSession, logger}
import org.apache.spark.sql.SparkSession

object Gold extends LayerEntity{

  private val entityMap: Map[String, MovieEntityObject] = Map(
    //TODO
  )
  override def execute(params: ApplicationParams): Unit = {
    val configFilePath = "silver/spark.conf"

    logger.info("Initializing Spark Session...")
    val spark: SparkSession = getSparkSession(configFilePath, params.appName)

    val entity = entityMap(params.entity())(spark, params)
    entity.execute()
  }
}
