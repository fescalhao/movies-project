package com.github.fescalhao.spark_project.movies.layers.silver.entities

import com.github.fescalhao.spark_project.core.{ApplicationParams, MasterEntity}
import com.github.fescalhao.spark_project.core.aws.s3.S3.{readCSV, writeDelta}
import com.github.fescalhao.spark_project.core.traits.{Entity, EntityObject}
import com.github.fescalhao.spark_project.movies.layers.silver.schemas.LinksSchema
import com.github.fescalhao.spark_project.movies.layers.silver.traits.MovieSilverEntity
import org.apache.log4j.Logger

class Links(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params) with Entity with MovieSilverEntity {
  val logger: Logger = Logger.getLogger(getClass.getName)
  override def execute(): Unit = {
    val sourcePath = s"$baseSourcePath/${params.entity()}_small.csv"
    val targetPath = s"$baseTargetPath/${params.entity()}"

    logger.info(s"Reading ${params.entity().capitalize} CSV file")
    val linksDF = readCSV(spark, schema, sourcePath, csvReadOptions)

    logger.info(s"Saving files in Silver layer in the path $targetPath")
    writeDelta(linksDF, targetPath)
  }
}

object Links extends EntityObject {
  override def apply(configFilePath: String, params: ApplicationParams): Entity = {
    val links = new Links(configFilePath, params)
    links.schema = LinksSchema.bronzeSchema
    links
  }
}
