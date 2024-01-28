package com.github.fescalhao.scala_project.layers.bronze

import com.github.fescalhao.scala_project.generics.ApplicationParams
import com.github.fescalhao.scala_project.layers.traits.{LayerEntity, EntityObject}


object Bronze extends LayerEntity {

  private val entityMap: Map[String, EntityObject] = Map(
    //TODO
  )

  override def execute(params: ApplicationParams): Unit = {
    val configFilePath = "bronze/spark.conf"

    val entity = entityMap(params.entity())(configFilePath, params)
    entity.execute()
  }
}
