package com.github.fescalhao.movies_project

import com.github.fescalhao.movies_project.silver.Silver

object Main extends Serializable{
  def main(args: Array[String]): Unit = {
    val p = Map("layer" -> "silver", "entity" -> "Ratings")

    if (p("layer") == "bronze") {
      //TODO
    } else if (p("layer") == "silver") {
      Silver.execute(p)
    } else if (p("layer") == "gold") {
      //TODO
    } else  {
      throw new RuntimeException("Layer parameter is incorrect or not present")
    }
  }

}


