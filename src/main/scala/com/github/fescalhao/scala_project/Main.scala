package com.github.fescalhao.scala_project

import com.github.fescalhao.scala_project.core.ApplicationParams
import com.github.fescalhao.scala_project.core.traits.LayerEntity
import com.github.fescalhao.scala_project.movies.layers.bronze.Bronze
import com.github.fescalhao.scala_project.movies.layers.gold.Gold
import com.github.fescalhao.scala_project.movies.layers.silver.Silver

object Main extends Serializable{
  private val layerMap: Map[String, LayerEntity] = Map(
    "bronze" -> Bronze,
    "silver" -> Silver,
    "gold" -> Gold
  )

  def main(args: Array[String]): Unit = {
    val params = new ApplicationParams(args)

    val layerEntity = layerMap(params.layer())
    layerEntity.execute(params)
  }
}
