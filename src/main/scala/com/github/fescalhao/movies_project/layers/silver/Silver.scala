package com.github.fescalhao.movies_project.layers.silver

import com.github.fescalhao.movies_project.generics.ApplicationParams
import com.github.fescalhao.movies_project.layers.silver.entities.{Credits, Ratings}
import com.github.fescalhao.movies_project.layers.traits.{LayerEntity, MovieEntityObject}

object Silver extends LayerEntity {

  private val entityMap: Map[String, MovieEntityObject] = Map(
    "ratings" -> Ratings,
    "credits" -> Credits
  )

  def execute(params: ApplicationParams): Unit = {
    val configFilePath = "silver/spark.conf"

    val entity = entityMap(params.entity())(configFilePath, params)
    entity.execute()
  }
}