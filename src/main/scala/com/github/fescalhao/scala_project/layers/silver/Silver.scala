package com.github.fescalhao.scala_project.layers.silver

import com.github.fescalhao.scala_project.generics.ApplicationParams
import com.github.fescalhao.scala_project.layers.silver.entities.{Credits, Ratings}
import com.github.fescalhao.scala_project.layers.traits.{LayerEntity, EntityObject}

object Silver extends LayerEntity {

  private val entityMap: Map[String, EntityObject] = Map(
    "ratings" -> Ratings,
    "credits" -> Credits
  )

  def execute(params: ApplicationParams): Unit = {
    val configFilePath = "silver/spark.conf"

    val entity = entityMap(params.entity())(configFilePath, params)
    entity.execute()
  }
}