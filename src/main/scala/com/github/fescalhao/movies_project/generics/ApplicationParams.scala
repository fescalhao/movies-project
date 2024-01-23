package com.github.fescalhao.movies_project.generics

import org.rogach.scallop.{ScallopConf, ScallopOption}

class ApplicationParams(arguments: Seq[String]) extends ScallopConf(arguments){
  var layer: ScallopOption[String] = opt[String](name = "layer", descr = "Data Layer", required = true)
  var entity: ScallopOption[String] = opt[String](name = "entity", descr = "Entity to be processed", required = true)
  verify()
}