package com.github.fescalhao.scala_project.movies.layers.silver.entities

import com.github.fescalhao.scala_project.core.aws.s3.S3.readCSV
import com.github.fescalhao.scala_project.core.traits.{Entity, EntityObject}
import com.github.fescalhao.scala_project.core.{ApplicationParams, MasterEntity}
import com.github.fescalhao.scala_project.movies.layers.silver.schemas.KeywordsSchema
import org.apache.log4j.Logger

class Keywords(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params) with Entity {
  override val logger: Logger = Logger.getLogger(getClass.getName)

  override def execute(): Unit = {
    val sourcePath = s"s3a://fescalhao-${params.project()}/bronze/ratings_small.csv"
    val targetPath = s"s3a://fescalhao-${params.project()}/silver/ratings_small"

    logger.info(s"Reading ${params.entity().capitalize} CSV file")
    val keywordsDF = readCSV(spark, schema, sourcePath)

    keywordsDF.write
      .format("delta")
      .mode("overwrite")
      .save(targetPath)
  }
}

object Keywords extends EntityObject {
  override def apply(configFilePath: String, params: ApplicationParams): Entity = {
    val keywords = new Keywords(configFilePath, params)
    keywords.schema = KeywordsSchema.bronzeSchema
    keywords
  }
}

