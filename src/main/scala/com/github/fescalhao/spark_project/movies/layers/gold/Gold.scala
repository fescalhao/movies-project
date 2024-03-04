package com.github.fescalhao.spark_project.movies.layers.gold

import com.github.fescalhao.spark_project.core.ApplicationParams
import com.github.fescalhao.spark_project.core.traits.{EntityObject, LayerEntity}

object Gold extends LayerEntity {

  val entityMap: Map[String, EntityObject] = Map(
    //TODO
  )

  override def execute(params: ApplicationParams): Unit = {
    val configFilePath = "gold/spark.conf"

    val entity = entityMap(params.entity())(configFilePath, params)
    entity.execute()
  }
}
