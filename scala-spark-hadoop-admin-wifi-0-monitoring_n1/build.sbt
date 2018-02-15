import Dependencies._

lazy val general = Seq(
  name := "scala-spark-hadoop-admin-WIFI-0-monitoring_N1",
  organization := "com.github.user",
  version := "1.0",
  scalaVersion := "2.11.11"
)

lazy val dependencies = Seq(
  sparkCore,// % "provided",
  sparkSql,// % "provided",
  slick,
  slickHikaricp,
  postgreSql,
  hadoopClient,// % "provided",
  configurator
)

lazy val root = (project in file("."))
  .settings(
    general,
    libraryDependencies ++= dependencies,
    dependencyOverrides ++= overrides,
    assemblyJarName in assembly := name.value + "-" + version.value + ".jar"
  )  
