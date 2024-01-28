package com.github.fescalhao.scala_project

import com.github.fescalhao.scala_project.generics.ApplicationParams
import com.github.fescalhao.scala_project.layers.bronze.Bronze
import com.github.fescalhao.scala_project.layers.gold.Gold
import com.github.fescalhao.scala_project.layers.silver.Silver
import com.github.fescalhao.scala_project.layers.traits.LayerEntity

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
