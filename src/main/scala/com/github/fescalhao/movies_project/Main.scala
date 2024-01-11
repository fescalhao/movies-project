package com.github.fescalhao.movies_project

import com.github.fescalhao.movies_project

object Main extends Serializable{

  private val projectPath = "com.github.fescalhao.movies_project"
  def main(args: Array[String]): Unit = {
    val p = Map("layer" -> "Silver")

    val layerObject = movies_project.getObject(s"$projectPath.${p("entity")}")

  }

}


