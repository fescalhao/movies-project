package com.github.fescalhao.scala_project.movies.layers.silver.entities

import com.github.fescalhao.scala_project.core.aws.s3.S3.readCSV
import com.github.fescalhao.scala_project.core.traits.{Entity, EntityObject}
import com.github.fescalhao.scala_project.core.{ApplicationParams, MasterEntity}
import com.github.fescalhao.scala_project.movies.layers.silver.schemas.MoviesMetadataSchema._
import com.github.fescalhao.scala_project.movies.layers.silver.traits.SilverEntity
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json}

class MoviesMetadata(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params) with Entity with SilverEntity {
  override val logger: Logger = Logger.getLogger(getClass.getName)

  override def execute(): Unit = {
    val sourcePath = s"data/movies_metadata.csv"
//    val sourcePath = s"$baseSourcePath/movies_metadata.csv"
//    val targetPath = s"$baseTargetPath/movies_metadata"

    logger.info(s"Reading ${params.entity().capitalize} CSV file")
    val moviesMetadataDF = readCSV(spark, schema, sourcePath)

    val moviesMetadataNewSchemaDF = moviesMetadataDF
      .withColumn("belongs_to_collection", from_json(col("belongs_to_collection"), belongsToCollectionSchema))
      .withColumn("genres", from_json(col("genres"), genresSchema))
      .withColumn("production_companies", from_json(col("production_companies"), productionCompaniesSchema))
      .withColumn("production_countries", from_json(col("production_countries"), productionCountriesSchema))
      .withColumn("spoken_languages", from_json(col("spoken_languages"), spokenLanguagesSchema))

    moviesMetadataNewSchemaDF.show(25, truncate = false)
  }
}

object MoviesMetadata extends EntityObject {
  override def apply(configFilePath: String, params: ApplicationParams): Entity = {
    val moviesMetadata = new MoviesMetadata(configFilePath, params)
    moviesMetadata.schema = bronzeSchema
    moviesMetadata
  }
}
