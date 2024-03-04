package com.github.fescalhao.spark_project.movies.layers.silver

import com.github.fescalhao.spark_project.core.ApplicationParams
import com.github.fescalhao.spark_project.core.traits.{EntityObject, LayerEntity}
import com.github.fescalhao.spark_project.movies.layers.silver.entities.{Credits, Keywords, Links, MoviesMetadata, Ratings}

import java.util.Date

object Silver extends LayerEntity {

  val entityMap: Map[String, EntityObject] = Map(
    "ratings" -> Ratings,
    "credits" -> Credits,
    "links" -> Links,
    "keywords" -> Keywords,
    "movies_metadata" -> MoviesMetadata
  )

  def execute(params: ApplicationParams): Unit = {
    val configFilePath = "silver/spark.conf"

    val entity = entityMap(params.entity())(configFilePath, params)
    entity.execute()
  }
}