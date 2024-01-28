package com.github.fescalhao.scala_project.layers.silver.entities

import com.github.fescalhao.scala_project.generics.ApplicationParams
import com.github.fescalhao.scala_project.{getCurrentTimeMillis, getSparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class MasterEntity(configFilePath: String, params: ApplicationParams, setAssumeRole: Boolean = true) {
  MasterEntity.logger.info("Initializing Spark Session...")
  val spark: SparkSession = getSparkSession(configFilePath, params.appName)

  MasterEntity.logger.info("Assuming AWS Role...")
  if (setAssumeRole) MasterEntity.setAssumeRole(spark, params)

}

object MasterEntity {
  val logger: Logger = Logger.getLogger(MasterEntity.getClass.getName)
  def apply(configFilePath: String, params: ApplicationParams): MasterEntity = {
    new MasterEntity(configFilePath, params)
  }

  private def setAssumeRole(spark: SparkSession, params: ApplicationParams): Unit = {
    val sparkConf: Configuration = spark.sparkContext.hadoopConfiguration

    val sessionName = s"movies_${params.layer()}_${params.entity()}_${getCurrentTimeMillis()}"

    sparkConf.set("fs.s3a.assumed.role.session.name", sessionName)
    sparkConf.set("fs.s3a.access.key", params.aws_user_access_key())
    sparkConf.set("fs.s3a.secret.key", params.aws_user_secret_key())
    sparkConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
    sparkConf.set("fs.s3a.assumed.role.arn", params.aws_assume_role_arn())
    sparkConf.set("fs.s3a.assumed.role.session.duration", params.aws_assume_role_duration())
    sparkConf.set("fs.s3a.assumed.role.sts.endpoint", params.aws_assume_sts_endpoint())
    sparkConf.set("fs.s3a.assumed.role.sts.endpoint.region", params.aws_assume_sts_endpoint_region())
  }
}
