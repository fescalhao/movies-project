import scala.collection.immutable.Seq

ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark_project",
    organization := "com.github.fescalhao"
  )

val sparkVersion = "3.5.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.delta" %% "delta-spark" % "3.0.0",
  "io.delta" % "delta-storage" % "3.0.0",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262", // % "provided"
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4" // % "provided"

)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

val otherDependencies = Seq(
  "org.rogach" %% "scallop" % "5.0.1"
)

libraryDependencies ++= sparkDependencies ++ testDependencies ++ otherDependencies

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}
