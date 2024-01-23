package com.github.fescalhao.movies_project.layers.traits

import com.github.fescalhao.movies_project.generics.ApplicationParams

trait LayerEntity {

  def execute(params: ApplicationParams): Unit

}
