package com.github.fescalhao.scala_project.movies.layers.silver.entities

import com.github.fescalhao.scala_project.core.{ApplicationParams, MasterEntity}
import com.github.fescalhao.scala_project.core.aws.s3.S3.{readCSV, writeDelta}
import com.github.fescalhao.scala_project.core.traits.{Entity, EntityObject}
import com.github.fescalhao.scala_project.movies.layers.silver.schemas.KeywordsSchema
import com.github.fescalhao.scala_project.movies.layers.silver.traits.MovieSilverEntity
import com.github.fescalhao.scala_project.movies.layers.silver.transformations.KeywordsTransformation
import org.apache.log4j.Logger

class Keywords(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params) with Entity with MovieSilverEntity {
  override val logger: Logger = Logger.getLogger(getClass.getName)

  override def execute(): Unit = {
    val sourcePath = s"$baseSourcePath/${params.entity()}.csv"
    val targetPath = s"$baseTargetPath/${params.entity()}"

    logger.info(s"Reading ${params.entity().capitalize} CSV file")
    val keywordsDF = readCSV(spark, schema, sourcePath, csvReadOptions)

    logger.info(s"Applying transformations...")
    val transformedKeywordsDF = KeywordsTransformation.transformKeywords(keywordsDF)

    logger.info(s"Saving files in Silver layer in the path $targetPath")
    writeDelta(transformedKeywordsDF, targetPath)
  }
}

object Keywords extends EntityObject {
  override def apply(configFilePath: String, params: ApplicationParams): Entity = {
    val keywords = new Keywords(configFilePath, params)
    keywords.schema = KeywordsSchema.bronzeSchema
    keywords
  }
}

