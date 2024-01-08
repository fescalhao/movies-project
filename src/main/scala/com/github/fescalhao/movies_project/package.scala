package com.github.fescalhao

import org.apache.spark.SparkConf

import java.util.Properties
import scala.io.Source

package object movies_project {
  def getSparkConf(appName: String): SparkConf = {
    val sparkConf = new SparkConf()
    val props: Properties = getSparkConfProperties

    props.forEach((k, v) => {
      sparkConf.set(k.toString, v.toString)
    })

    sparkConf.setAppName(appName)
  }

  private def getSparkConfProperties: Properties = {
    val props = new Properties()
    props.load(Source.fromFile("src/main/resources/spark.conf").bufferedReader())

    props
  }
}
