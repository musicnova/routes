package com.github.user.utils

import org.apache.spark.sql.SparkSession
import com.github.user.utils.Configuration._

import scala.annotation.tailrec

trait SparkApp extends App {

  lazy val spark = createSparkSession

  private def createSparkSession: SparkSession = {

    @tailrec
    def createBuilder(config: Seq[(String, String)], initBuilder: SparkSession.Builder): SparkSession.Builder =
      if (config.isEmpty) initBuilder
      else
        createBuilder(config.tail, initBuilder.config(config.head._1, config.head._2))

    createBuilder(
      config = initConf,
      initBuilder = SparkSession.builder()
        .appName(sparkAppName)
        .master(sparkMaster)
    )
      .getOrCreate()

  }
}
