package com.github.fescalhao.movies_project

import com.github.fescalhao.movies_project.silver.Silver

object Main extends Serializable{
  def main(args: Array[String]): Unit = {
    val p = Map("layer" -> "silver", "entity" -> "Ratings")

    executeLayer(p("layer"), p)
  }

  private def executeLayer(layer: String, params: Map[String, String]): Unit = layer match {
    case "bronze" => //TODO
    case "silver" => Silver.execute(params)
    case "gold" => //TODO
    case _ => throw new RuntimeException("Layer parameter is incorrect or not present")
  }

}


