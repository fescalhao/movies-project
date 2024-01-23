package com.github.fescalhao
import org.apache.log4j.Logger

import scala.reflect.runtime.{universe => ru}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import java.util.Properties
import scala.io.Source

package object movies_project {
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
    println(configFilePath)
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

  /** Used to instantiate and execute a method through reflection using the Scala Reflection API.
   * Reference: https://www.baeldung.com/scala/reflection-api
   *
   * @param path Entity class path
   * @param methodName Name of the method to be executed in the Entity class. Defaults to "execute"
   * @return Unit -> The execution of the method "execute" from the specific Entity class
   */
  def executeEntity(path: String, params: Map[String, String], methodName: String = "execute"): Any = {
    val mirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol: ru.ClassSymbol = mirror.classSymbol(Class.forName(path))
    val consMethodSymbol = classSymbol.primaryConstructor.asMethod

    val classMirror = mirror.reflectClass(classSymbol)
    val classConstructorMirror = classMirror.reflectConstructor(consMethodSymbol)

    val classInstance = classConstructorMirror.apply(params)
    val classInstanceMirror = mirror.reflect(classInstance)

    val methodSymbol = classSymbol.info.decl(ru.TermName(methodName)).asMethod
    val method = classInstanceMirror.reflectMethod(methodSymbol)

    method.apply()
  }

  def getEntityClassPath(params: Map[String, String]): String = {
    val projectPath = "com.github.fescalhao.movies_project"
    s"$projectPath.${params("layer")}.entities.${params("entity")}"
  }
}
