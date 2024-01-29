package com.github.fescalhao.scala_project.movies.layers.silver.entities

import com.github.fescalhao.scala_project.core.aws.s3.S3.readCSV
import com.github.fescalhao.scala_project.core.{ApplicationParams, MasterEntity}
import com.github.fescalhao.scala_project.core.traits.{Entity, EntityObject}
import com.github.fescalhao.scala_project.movies.layers.silver.schemas.CreditsSchema._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json}

class Credits(configFilePath: String, params: ApplicationParams) extends MasterEntity(configFilePath, params, false) with Entity {

  override val logger: Logger = Logger.getLogger(getClass.getName)

  def execute(): Unit = {
    val path = "s3a://fescalhao-movies/bronze/credits.csv"

    val creditsDF = readCSV(spark, schema, path)

    val creditsNewSchemaDF = creditsDF
      .withColumn("cast", from_json(col("cast"), castSchema))
      .withColumn("crew", from_json(col("crew"), crewSchema))

    creditsNewSchemaDF.show(25, truncate = false)
  }
}

object Credits extends EntityObject {
  def apply(configFilePath: String, params: ApplicationParams): Credits = {
    val credits = new Credits(configFilePath, params)
    credits.schema = bronzeSchema
    credits
  }
}
