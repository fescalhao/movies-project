package com.github.fescalhao.spark_project.core.traits

import com.github.fescalhao.spark_project.core.ApplicationParams

trait LayerEntity {

  val entityMap: Map[String, EntityObject]

  def execute(params: ApplicationParams): Unit

}
