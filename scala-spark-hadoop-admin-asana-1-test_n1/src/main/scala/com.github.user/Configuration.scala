package com.github.user.utils

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

object Configuration {

  private val conf: Config = ConfigFactory.load()

  val sparkAppName: String = conf.getString("spark.appName")
  val sparkMaster: String = conf.getString("spark.master")
  val initConf: Seq[(String, String)] = conf.getObjectList("spark.startConfiguration")
    .asScala
    .flatMap(cfg => cfg.unwrapped().asScala)
    .map { case (k, v) => (k, v.asInstanceOf[String]) }
}
