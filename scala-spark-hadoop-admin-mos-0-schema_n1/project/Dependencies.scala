import sbt._

object Dependencies {

  lazy val sparkVersion = "2.0.0"
  lazy val hadoopVersion = "2.7.3"
  lazy val opencsvVersion = "1.3.5"
  lazy val log4jVersion = "1.6.4"
  lazy val postgreVersion = "9.4-1204-jdbc4"

  lazy val postgresDriver = "org.postgresql" % "postgresql" % postgreVersion
  lazy val log4jApi = "org.slf4j" % "slf4j-api" % log4jVersion
  lazy val log4jCore = "org.slf4j" % "slf4j-log4j12" % log4jVersion
  lazy val openCsv = "com.github.tototoshi" %% "scala-csv" % opencsvVersion
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  lazy val hadoopClient = "org.apache.hadoop" % "hadoop-client" % hadoopVersion
  lazy val configurator = "com.typesafe" % "config" % "1.3.1"

  lazy val overrides = Seq(
    "commons-net" % "commons-net" % "3.1",
    "com.google.guava" % "guava" % "14.0.1",
    "com.google.code.findbugs" % "jsr305" % "3.0.0",
    "io.netty" % "netty" % "3.8.0.Final",
    "io.netty" % "netty-all" % "4.0.29.Final"
  )
}
