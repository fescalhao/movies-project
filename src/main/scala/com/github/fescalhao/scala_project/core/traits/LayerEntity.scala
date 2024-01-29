package com.github.fescalhao.scala_project.core.traits

import com.github.fescalhao.scala_project.core.ApplicationParams

trait LayerEntity {

  val entityMap: Map[String, EntityObject]

  def execute(params: ApplicationParams): Unit

}
