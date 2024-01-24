package com.github.fescalhao.movies_project.layers.gold

import com.github.fescalhao.movies_project.generics.ApplicationParams
import com.github.fescalhao.movies_project.layers.traits.{LayerEntity, MovieEntityObject}

object Gold extends LayerEntity {

  private val entityMap: Map[String, MovieEntityObject] = Map(
    //TODO
  )

  override def execute(params: ApplicationParams): Unit = {
    val configFilePath = "gold/spark.conf"

    val entity = entityMap(params.entity())(configFilePath, params)
    entity.execute()
  }
}
