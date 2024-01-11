package com.github.fescalhao

import scala.reflect.runtime.{universe => ru}
import org.apache.spark.SparkConf

import java.util.Properties
import scala.io.Source

package object movies_project {
  def getSparkConf(configFilePath: String, appName: String): SparkConf = {
    val sparkConf = new SparkConf()
    val props: Properties = getSparkConfProperties(configFilePath)

    props.forEach((k, v) => {
      sparkConf.set(k.toString, v.toString)
    })

    sparkConf.setAppName(appName)
  }

  private def getSparkConfProperties(configFilePath: String): Properties = {
    val props = new Properties()
    props.load(Source.fromFile(configFilePath).bufferedReader())

    props
  }

  def getObject(path: String) = {
    val mirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol: ru.ClassSymbol = mirror.classSymbol(Class.forName(path))
    val consMethodSymbol = classSymbol.primaryConstructor.asMethod

    val classMirror = mirror.reflectClass(classSymbol)
    val classConstructorMirror = classMirror.reflectConstructor(consMethodSymbol)

    classConstructorMirror.apply()
  }
}
