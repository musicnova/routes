import Dependencies._

lazy val general = Seq(
  name := "scala-spark-hadoop-admin-asana-1-test_N1",
  organization := "com.github.user",
  version := "1.0",
  scalaVersion := "2.11.11"
)

lazy val dependencies = Seq(
  sparkCore, // % "provided",
  sparkSql, // % "provided",
  hadoopClient, // % "provided",
  configurator
)

lazy val root = (project in file("."))
  .settings(
    general,
    libraryDependencies ++= dependencies,
    dependencyOverrides ++= overrides,
    assemblyJarName in assembly := name.value + "-" + version.value + ".jar"
  )  
