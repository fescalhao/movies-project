package com.github.fescalhao.movies_project.silver

import com.github.fescalhao.movies_project.{executeEntity, getEntityClassPath}

object Silver {
  def execute(params: Map[String, String]): Unit ={

    val classPath = getEntityClassPath(params)

    executeEntity(path = classPath, params)

  }

}
