package com.github.fescalhao.scala_project.movies.layers.silver.entities

import com.github.fescalhao.scala_project.core.{ApplicationParams, MasterEntity}
import com.github.fescalhao.scala_project.core.aws.s3.S3.{readCSV, writeDelta}
import com.github.fescalhao.scala_project.core.traits.{Entity, EntityObject}
import com.github.fescalhao.scala_project.movies.layers.silver.schemas.CreditsSchema.bronzeSchema
import com.github.fescalhao.scala_project.movies.layers.silver.traits.MovieSilverEntity
import com.github.fescalhao.scala_project.movies.layers.silver.transformations.CreditsTransformation
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, explode}

class Credits(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params) with Entity with MovieSilverEntity {

  override val logger: Logger = Logger.getLogger(getClass.getName)

  def execute(): Unit = {
    val sourcePath = s"$baseSourcePath/${params.entity()}.csv"
    val targetPath = s"$baseTargetPath/${params.entity()}"

    val creditsDF = readCSV(spark, schema, sourcePath, csvReadOptions)

    val transformedCreditsDF =  CreditsTransformation.transformCredits(creditsDF).cache()

    val castDF = transformedCreditsDF.select(col("id"), explode(col("cast")).alias("cast"))
    val crewDF = transformedCreditsDF.select(col("id"), explode(col("crew")).alias("crew"))

    writeDelta(transformedCreditsDF, s"$targetPath/complete")
    writeDelta(castDF, s"$targetPath/cast")
    writeDelta(crewDF, s"$targetPath/crew")
  }
}

object Credits extends EntityObject {
  def apply(configFilePath: String, params: ApplicationParams): Credits = {
    val credits = new Credits(configFilePath, params)
    credits.schema = bronzeSchema
    credits
  }
}
