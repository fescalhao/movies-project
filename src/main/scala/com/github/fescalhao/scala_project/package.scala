package com.github.fescalhao

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, DateTimeZone}

import java.util.Properties
import scala.io.Source

package object scala_project {

  private def getSparkConf(configFilePath: String, appName: String): SparkConf = {
    val sparkConf = new SparkConf()
    val props: Properties = getSparkConfProperties(configFilePath)

    props.forEach((k, v) => {
      sparkConf.set(k.toString, v.toString)
    })

    sparkConf.setAppName(appName)
  }

  private def getSparkConfProperties(configFilePath: String): Properties = {
    val props = new Properties()
    props.load(Source.fromResource(configFilePath).bufferedReader())

    props
  }

  def getSparkSession(configFilePath: String, appName: String): SparkSession = {
    val sparkConf: SparkConf = getSparkConf(
      configFilePath = configFilePath,
      appName = appName
    )

    SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()
  }

  def getCurrentTimeMillis(dateTimeZone: DateTimeZone = DateTimeZone.UTC): Long = DateTime.now(dateTimeZone).getMillis
}
