package com.github.fescalhao.spark_project

import com.github.fescalhao.spark_project.core.ApplicationParams
import com.github.fescalhao.spark_project.core.traits.LayerEntity
import com.github.fescalhao.spark_project.movies.layers.bronze.Bronze
import com.github.fescalhao.spark_project.movies.layers.gold.Gold
import com.github.fescalhao.spark_project.movies.layers.silver.Silver

object Main {
  def main(args: Array[String]): Unit = {
    val layerMap: Map[String, LayerEntity] = Map(
      "bronze" -> Bronze,
      "silver" -> Silver,
      "gold" -> Gold
    )

    val params = new ApplicationParams(args)

    val layerEntity = layerMap(params.layer())
    layerEntity.execute(params)
  }
}
