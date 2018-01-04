import Dependencies._

lazy val general = Seq(
  name := "scala-spark-hadoop-admin-MOS-0-schema_N1",
  organization := "com.github.user",
  version := "1.0",
  scalaVersion := "2.11.11"
)

lazy val dependencies = Seq(
  log4jApi,
  log4jCore,
  sparkCore % "provided",
  sparkSql % "provided",
  hadoopClient % "provided",
  openCsv, //% "provided",
  configurator
)

lazy val root = (project in file("."))
  .settings(
    general,
    libraryDependencies ++= dependencies,
    dependencyOverrides ++= overrides,
    assemblyJarName in assembly := name.value + "-" + version.value + ".jar"
  )  
