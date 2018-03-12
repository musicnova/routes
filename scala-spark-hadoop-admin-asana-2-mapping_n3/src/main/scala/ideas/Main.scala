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
    lazy val spark: SparkSession = SparkSession.builder().appName("admin_ASANA-3_mapping_N3")
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



    import org.apache.commons.lang.time.DateUtils
    import java.text.SimpleDateFormat
    val myFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cur = myFormat.format(new java.util.Date())
    val prev = myFormat.format(DateUtils.addDays(new java.util.Date(), -90))
    val dfTest = spark.sqlContext.sql("SELECT '" + cur + "' as ts, '" + prev + "' as pr")
    // hiveContext.sql("drop table if exists sandbox.yurbasov_nightly_wifi")
    // dfTest.write.saveAsTable("sandbox.yurbasov_nightly_wifi")



    val dfSecretRegno = spark.sqlContext.sql("SELECT '" + "1356c440a1225f733051fe1251ebcee0" + "' as regno_hash" +
      ", '" + "2017-01-31 23:59:52.0" + "' as regno")
    val dfDataParking = spark.sqlContext.sql("SELECT '" + "2016-01-01 05:34:40.0" + "' as parkingstart" +
      ", '" + "415" + "' as phoneno" +
      ", '" + "2515084216" + "' as carno"
    )
    val dfDataCodd = spark.sqlContext.sql("SELECT '" + "90" + "' as v_azimut" +
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
    // val dfSecretRegno = hiveContext.read.table("maxima_data.metro")
    // val dfDataParking = hiveContext.read.table("metro_data.entries")
    // val dfDataCodd = hiveContext.read.table("codd_data.codd")



    val dfBase1 = dfDataCodd.select('v_regno).where('v_time_check > lit(prev)).filter('v_time_check < lit(cur))
    val dfPre1 = dfBase1.select('v_regno).groupBy('v_regno).agg(max('v_regno) as "v_regno0")
    val dfPar1 = dfDataParking.select('phoneno, 'carno, 'parkingstart, lit(1) as "total")
      .groupBy('phoneno, 'carno)
      .agg(max('parkingstart) as "max_date", sum('total) as "cty")
      .select('phoneno, 'carno, 'max_date, 'cty, row_number().over(
        Window.partitionBy('carno).orderBy(col("cty").desc)).as("rn"))
      .select('phoneno, 'carno).where('rn === lit(1))
    val dfExt1 = dfPre1.join(dfSecretRegno, dfPre1.col("v_regno0") === dfSecretRegno.col("regno_hash"), "left_outer")
        .select('v_regno0, 'regno_hash, 'regno)
          .groupBy('v_regno0, 'regno_hash, 'regno).agg(max('v_regno0) as "v_regno2",
      max('regno_hash) as "regno_hash2",
      max('regno) as "regno2")
    val dfRes1 = dfExt1.join(dfPar1, dfExt1.col("v_regno2") === dfPar1("carno"), "inner")
      .filter('carno.isNotNull).select('v_regno2, 'regno, 'phoneno)
          .groupBy('v_regno2, 'regno, 'phoneno)
      .agg(max('v_regno2) as "v_regno",
        max('regno) as "regno",
        max('phoneno) as "phoneno")



    // val dfResult = empty
    /*
    val dfResult1 = dfSecretRegno.select('fdt cast StringType, lit(1) as "v_total",
      lit("maxima_data.metro") as "v_what")
      .select(unix_timestamp('fdt cast StringType, "yyyy-MM-dd") cast TimestampType as "v_when",
        'v_total, 'v_what).groupBy('v_when, 'v_what).agg(sum("v_total").as("v_total"))
      .withColumn("v_stamp", unix_timestamp(lit(cur), "yyyy-MM-dd") cast TimestampType)
    val dfResult2 = dfDataParking.select('point_time cast StringType, lit(1) as "v_total",
      lit("metro_data.entries") as "v_what")
      .select(unix_timestamp('point_time cast StringType, "yyyy-MM-dd") cast TimestampType as "v_when",
        'v_total, 'v_what).groupBy('v_when, 'v_what).agg(sum("v_total").as("v_total"))
      .withColumn("v_stamp", unix_timestamp(lit(cur), "yyyy-MM-dd") cast TimestampType)
    val dfResult3 = dfDataCodd.select('v_time_check cast StringType, lit(1) as "v_total",
      lit("codd_data.codd") as "v_what")
      .select(unix_timestamp('v_time_check cast StringType, "yyyy-MM-dd") cast TimestampType as "v_when",
        'v_total, 'v_what).groupBy('v_when, 'v_what).agg(sum("v_total").as("v_total"))
      .withColumn("v_stamp", unix_timestamp(lit(cur), "yyyy-MM-dd") cast TimestampType)
*/

    val dfResult = dfRes1
    dfResult.show()

    dfResult.coalesce(1)
      .write.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .save("/home/user/ALBINA/YURIY/CODD_MAPPING")

    // TODO: spark.sparkContext.stop
    // success match { case Failure(e) => throw e case _ => }
  }
}