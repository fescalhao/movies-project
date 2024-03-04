package com.github.fescalhao.spark_project.movies.layers.silver.entities

import com.github.fescalhao.spark_project.core.{ApplicationParams, MasterEntity}
import com.github.fescalhao.spark_project.core.aws.s3.S3.{readCSV, writeDelta}
import com.github.fescalhao.spark_project.core.traits.{Entity, EntityObject}
import com.github.fescalhao.spark_project.movies.layers.silver.schemas.MoviesMetadataSchema._
import com.github.fescalhao.spark_project.movies.layers.silver.traits.MovieSilverEntity
import com.github.fescalhao.spark_project.movies.layers.silver.transformations.MoviesMetadataTransformation
import org.apache.log4j.Logger

class MoviesMetadata(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params) with Entity with MovieSilverEntity {
  override val logger: Logger = Logger.getLogger(getClass.getName)

  override def execute(): Unit = {
    val sourcePath = s"$baseSourcePath/${params.entity()}.csv"
    val targetPath = s"$baseTargetPath/${params.entity()}"

    logger.info(s"Reading ${params.entity().capitalize} CSV file")
    val moviesMetadataDF = readCSV(spark, schema, sourcePath, csvReadOptions)

    logger.info(s"Applying transformations...")
    val transformedMoviesMetadataDF = MoviesMetadataTransformation.transformMoviesMetadata(moviesMetadataDF)

    logger.info(s"Saving files in Silver layer in the path $targetPath")
    writeDelta(transformedMoviesMetadataDF, targetPath)
  }
}

object MoviesMetadata extends EntityObject {
  override def apply(configFilePath: String, params: ApplicationParams): Entity = {
    val moviesMetadata = new MoviesMetadata(configFilePath, params)
    moviesMetadata.schema = bronzeSchema
    moviesMetadata
  }
}
