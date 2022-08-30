name := "regime"
organization := "com.github.jacobbishopxy"
version := "1.0"

scalaVersion := "2.12.15"

val sparkVersion = "3.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
