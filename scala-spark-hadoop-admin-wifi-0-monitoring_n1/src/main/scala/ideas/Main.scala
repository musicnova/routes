package ideas

import org.apache.spark.sql.SparkSession

object Main extends Serializable {

  def initLog4j(): Unit = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    println("Welcome to Spark! hdp-master2.mos.ru:8020/user")
    initLog4j()
    lazy val spark: SparkSession = SparkSession.builder().appName("admin_WIFI-0_monitoring_N1")
      //.config("spark.sql.warehouse.dir", "/tmp_spark/spark-warehouse")
      //.config("spark.local.dir", "/tmp_spark/spark-temp")
      .config("spark.driver.maxResultSize", "300g")
      //.config("spark.worker.cleanup.enabled", "true")
      //.config("spark.worker.cleanup.interval", "900")
      //.getOrCreate
       .master("local[*]").getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // %spark
    import org.apache.spark._
    import org.apache.spark.sql._
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.types._



    import java.text.SimpleDateFormat
    val myFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cur = myFormat.format(new java.util.Date())
    val dfTest = spark.sqlContext.sql("SELECT '" + cur + "' as ts")
    // hiveContext.sql("drop table if exists sandbox.yurbasov_nightly_wifi")
    // dfTest.write.saveAsTable("sandbox.yurbasov_nightly_wifi")



    val dfMaximaMetro = spark.sqlContext.sql("SELECT '" + "1356c440a1225f733051fe1251ebcee0" + "' as hash_msisdn" +
      ", '" + "2017-01-31 23:59:52.0" + "' as fdt" +
      ", '" + "2017-02-01 00:07:38.0" + "' as tdt" +
      ", '" + "94" + "' as startstid" +
      ", '" + "92" + "' as stopstid" +
      ", '" + "2017-01-31" + "' as day"
    )
    val dfMetroEntries = spark.sqlContext.sql("SELECT '" + "2016-01-01 05:34:40.0" + "' as point_time" +
      ", '" + "415" + "' as entrance_id" +
      ", '" + "2515084216" + "' as ticket_number" +
      ", '" + "14639824337866630" + "' as uid" +
      ", '" + "4381" + "' as ticket_type" +
      ", '" + "0" + "' as entry_type" +
      ", '" + "0" + "' as ride_count" +
      ", '" + "14" + "' as rides_left" +
      ", '" + "2016-01-01" + "' as day"
    )
    val dfCoddCodd = spark.sqlContext.sql("SELECT '" + "90" + "' as v_azimut" +
      ", '" + "30012" + "' as v_camera" +
      ", '" + "Боровское ш., д.13А, с.1, в центр, Солнцево р-н, г. Москва" + "' as v_camera_place" +
      ", '" + "1" + "' as v_direction" +
      ", '" + "37.380722" + "' as v_gps_x" +
      ", '" + "55.65182" + "' as v_gps_y" +
      ", '" + "0" + "' as v_lane_num" +
      ", '" + "null" + "' as v_parking_num" +
      ", '" + "null" + "' as v_parking_zone" +
      ", '" + "100.0" + "' as v_recognition_accuracy" +
      ", '" + "null" + "' as v_regno" +
      ", '" + "d05c8fa46909cda179daaa2f2641131c" + "' as v_regno_color_id" +
      ", '" + "171" + "' as v_regno_country_id" +
      ", '" + "48.0" + "' as v_speed" +
      ", '" + "60.0" + "' as v_speed_limit" +
      ", '" + "2017-09-01 09:09:29.0" + "' as v_time_check" +
      ", '" + "2017-09-01" + "' as day"
    )
    // val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    // val dfMaximaMetro = hiveContext.read.table("maxima_data.metro")
    // val dfMetroEntries = hiveContext.read.table("metro_data.entries")
    // val dfCoddCodd = hiveContext.read.table("codd_data.codd")



    // val dfResult = empty
    val dfResult1 = dfMaximaMetro.select('fdt cast StringType, lit(1) as "total",
      lit("maxima_data.metro") as "what")
      .select(unix_timestamp('fdt cast StringType, "yyyy-MM-dd") cast TimestampType as "when",
        'total, 'what).groupBy('when, 'what).agg(sum("total").as("total"))
      .withColumn("stamp", unix_timestamp(lit(cur), "yyyy-MM-dd") cast TimestampType)
    val dfResult2 = dfMetroEntries.select('point_time cast StringType, lit(1) as "total",
      lit("metro_data.entries") as "what")
      .select(unix_timestamp('point_time cast StringType, "yyyy-MM-dd") cast TimestampType as "when",
        'total, 'what).groupBy('when, 'what).agg(sum("total").as("total"))
      .withColumn("stamp", unix_timestamp(lit(cur), "yyyy-MM-dd") cast TimestampType)
    val dfResult3 = dfCoddCodd.select('v_time_check cast StringType, lit(1) as "total",
      lit("codd_data.codd") as "what")
      .select(unix_timestamp('v_time_check cast StringType, "yyyy-MM-dd") cast TimestampType as "when",
        'total, 'what).groupBy('when, 'what).agg(sum("total").as("total"))
      .withColumn("stamp", unix_timestamp(lit(cur), "yyyy-MM-dd") cast TimestampType)


    val dfResult = dfResult1.union(dfResult2).union(dfResult3)
    dfResult.show()

    dfResult.coalesce(1)
      .write.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .save("/home/user/ALBINA/YURIY/NIGHTLY")

    // TODO: spark.sparkContext.stop
    // success match { case Failure(e) => throw e case _ => }
  }
}
