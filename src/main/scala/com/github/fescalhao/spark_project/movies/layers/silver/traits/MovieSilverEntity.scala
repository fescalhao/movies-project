package com.github.fescalhao.spark_project.movies.layers.silver.traits

trait MovieSilverEntity {
  val baseSourcePath = s"s3a://fescalhao-movies/bronze"
  val baseTargetPath = s"s3a://fescalhao-movies/silver"

  val csvReadOptions: Map[String, String] = Map(
    "header" -> "true",
    "escape" -> "\""
  )
}
