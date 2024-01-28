package com.github.fescalhao.scala_project.layers.traits

import com.github.fescalhao.scala_project.generics.ApplicationParams

trait LayerEntity {

  def execute(params: ApplicationParams): Unit

}
