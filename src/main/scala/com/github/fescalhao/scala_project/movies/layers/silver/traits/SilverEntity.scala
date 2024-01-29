package com.github.fescalhao.scala_project.movies.layers.silver.traits

trait SilverEntity {

  val baseSourcePath = s"s3a://fescalhao-movies/bronze"
  val baseTargetPath = s"s3a://fescalhao-movies/silver"
}
