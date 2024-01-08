import scala.collection.immutable.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "movies",
    organization := "com.github.fescalhao"
  )

val sparkVersion = "3.5.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies
