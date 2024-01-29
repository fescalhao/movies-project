package com.github.fescalhao.scala_project.movies.layers.silver.entities

import com.github.fescalhao.scala_project.core.aws.s3.S3.readCSV
import com.github.fescalhao.scala_project.core.traits.{Entity, EntityObject}
import com.github.fescalhao.scala_project.core.{ApplicationParams, MasterEntity}
import com.github.fescalhao.scala_project.movies.layers.silver.schemas.LinksSchema
import org.apache.log4j.Logger

class Links(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params) with Entity {
  val logger: Logger = Logger.getLogger(getClass.getName)
  override def execute(): Unit = {
    val sourcePath = s"s3a://fescalhao-${params.project()}/bronze/links_small.csv"
    val targetPath = s"s3a://fescalhao-${params.project()}/silver/links_small"

    logger.info(s"Reading ${params.entity().capitalize} CSV file")
    val linksDF = readCSV(spark, schema, sourcePath)

    linksDF.write
      .format("delta")
      .mode("overwrite")
      .save(targetPath)
  }
}

object Links extends EntityObject {
  override def apply(configFilePath: String, params: ApplicationParams): Entity = {
    val links = new Links(configFilePath, params)
    links.schema = LinksSchema.bronzeSchema
    links
  }
}
