package com.github.fescalhao.scala_project.layers.gold

import com.github.fescalhao.scala_project.generics.ApplicationParams
import com.github.fescalhao.scala_project.layers.traits.{LayerEntity, EntityObject}

object Gold extends LayerEntity {

  private val entityMap: Map[String, EntityObject] = Map(
    //TODO
  )

  override def execute(params: ApplicationParams): Unit = {
    val configFilePath = "gold/spark.conf"

    val entity = entityMap(params.entity())(configFilePath, params)
    entity.execute()
  }
}
