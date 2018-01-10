package ideas

import java.nio.file.Paths
import java.sql.Connection
import java.util.{Calendar, Properties}

import ideas.X0Demo.X.XType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object X0Demo extends Serializable {
  def initLog4j(): Unit = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  object X extends Enumeration {
    type XType = Value
    val EDWEX, ITASK, T1_METRO_GEO2, T2_MAXIMA, T3_DATA_LINE_CODES, T4_DATA_STATION_CODES,
    T5_DATA_ENTRANCE_STATION_CODES, T6_DATA_PARKING_CODES,
    T7_YANDEX_LABEL_CODES, T8_YANDEX_LINE_CODES, T9_YANDEX_STATION_CODES,
    T10_YANDEX_LINK_CODES, T11_YANDEX_TRANSFER_CODES,
    O1_CASE_OWNERS, O2_CASE_RECORDS, O3_CASE_GRAPHS, O4_CASE_STATS, O5_CASE_ROUTES, O6_CASE_PROFILES = Value
  }

  case class My (dat: DataFrame, edw: DataFrame)

  case class Db (tbl: String, how: Properties)

  case class M (frames: Map[X.XType, My], alts: Map[X.XType, Db])

  def main(args: Array[String]): Unit = {
    println("Welcome to Spark!")
    initLog4j()
    val dataDir = "/home/user/KHD/001"
    val wareDir = "/home/user/SPARK/HOUSE"
    val tempDir = "/home/user/SPARK/WORK"
    val workDir = "/home/user/SPARK/WORK/X0"
    lazy val spark: SparkSession = SparkSession.builder().appName("basov_BDT-21_unlimit_upsell_X5")
      .config("spark.sql.warehouse.dir", wareDir)
      .config("spark.local.dir", tempDir)
      .config("spark.driver.maxResultSize", "3000000g")
      .config("spark.worker.cleanup.enabled", "true")
      .config("spark.worker.cleanup.interval", "900")
      // .enableHiveSupport()
      .master("local[*]").getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    import org.apache.spark.sql.functions._

    import scala.util.{Try, Failure}
    val success = Try {
      val testCondition: Column = lit(1000) === lit(1000)

      val baseEdwexDF = spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", "\t")
        .load(Paths.get("/home/user/", "CODE", "edwex.csv").toString).orderBy('_c0 cast LongType, '_c1 cast LongType)
      baseEdwexDF.show(3, truncate = false)
//      |_c0|_c1|_c2       |_c3      |_c4         |
//      +---+---+----------+---------+------------+
//      |1  |1  |METRO_GEO2|LINE     |VARCHAR65535|
//        |1  |2  |METRO_GEO2|LINECOLOR|VARCHAR65535|
//        |1  |3  |METRO_GEO2|NAME     |VARCHAR65535|

      val zeroITaskSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "0").collect.map(row => StructField(row.getString(0), StringType))
      )
      val zeroITaskDF = spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "true").option("delimiter", "\t")
        .load(Paths.get("/home/user/", "CODE", "itask.csv").toString).rdd, zeroITaskSchema)
      zeroITaskDF.show(3, truncate = false)
//      |N  |LOGIC |NAME               |LEVEL|SRC_FIELD|SRC_SYSTEM |SRC_LOAD|SRC_TABLE|KHD_TABLE|REGION|COMMENT|
//      +---+------+-------------------+-----+---------+-----------+--------+---------+---------+------+-------+
//      |1  |select|***CASE_OWNERS.CODE|2    |n/a      |data.mos.ru|000001  |dual     |dual     |      |-      |
//      |2  |select|CASE_OWNERS.ID     |2    |n/a      |data.mos.ru|000001  |dual     |dual     |      |-      |
//      |3  |select|CASE_OWNERS.SCHEMA |2    |n/a      |data.mos.ru|000001  |dual     |dual     |      |-      |

      val oneMetroGeoSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "1").collect.map(row => StructField(row.getString(0), StringType))
      )
      val oneMetroGeoDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ",")
        .load(Paths.get(dataDir, "000001", "metro_geo2_20180104_20180104.csv").toString).rdd, oneMetroGeoSchema)
      oneMetroGeoDF.show(3, truncate = false)
//      |LINE       |LINECOLOR|NAME       |LATITUDE |LONGITUDE|ORDER|
//      +-----------+---------+-----------+---------+---------+-----+
//      |Калининская|FFCD1C   |Новокосино |55.745113|37.864052|0    |
//        |Калининская|FFCD1C   |Новогиреево|55.752237|37.814587|1    |
//        |Калининская|FFCD1C   |Перово     |55.75098 |37.78422 |2    |

      val twoMaximaSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "2").collect.map(row => StructField(row.getString(0), StringType))
      )
      val twoMaximaDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ",")
        .load(Paths.get(dataDir, "000002", "maxima_v20170615_20180104_20180104.csv").toString).rdd, twoMaximaSchema)
      twoMaximaDF.show(3, truncate = false)
//      |LINE_NUMBER|STATION_NUMBER_ABS|STATION_NAME_LONG_RU  |STATION_NAME_LONG_EN      |
//      +-----------+------------------+----------------------+--------------------------+
//      |1          |1                 |Бульвар Рокоссовского |Bulvar Rokossovskogo      |
//        |1          |2                 |Черкизовская          |Cherkizovskaya            |
//        |1          |3                 |Преображенская площадь|Preobrazhenskaya Ploshchad|

      val threeDataLineCodesSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "3").collect.map(row => StructField(row.getString(0), StringType))
      )
      val threeDataLineCodesDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ";")
        .load(Paths.get(dataDir, "000003",
          "data_line_codes_v20171224_20180104_20180104.csv").toString).rdd, threeDataLineCodesSchema)
      threeDataLineCodesDF.show(3, truncate = false)
//      |LINE                     |ID |GLOBAL_ID|STATUS   |
//      +-------------------------+---+---------+---------+
//      |Сокольническая линия     |1  |62921363 |действует|
//        |Замоскворецкая линия     |2  |62921364 |действует|
//        |Арбатско-Покровская линия|3  |62921365 |действует|

      val fourDataStationCodesSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "4").collect.map(row => StructField(row.getString(0), StringType))
      )
      val fourDataStationCodesDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ";")
        .load(Paths.get(dataDir, "000004",
          "data_station_codes_v20171224_20180104_20180104.csv").toString).rdd, fourDataStationCodesSchema)
      fourDataStationCodesDF.show(3, truncate = false)
//      |STATION      |LINE                     |ADMAREA                                |GLOBAL_ID|DISTRICT                 |STATUS   |ID |
//      +-------------+-------------------------+---------------------------------------+---------+-------------------------+---------+---+
//      |Третьяковская|Калининская линия        |Центральный административный округ     |58701962 |район Замоскворечье      |действует|136|
//        |Медведково   |Калужско-Рижская линия   |Северо-Восточный административный округ|58701963 |район Северное Медведково|действует|86 |
//        |Первомайская |Арбатско-Покровская линия|Восточный административный округ       |58701964 |район Измайлово          |действует|41 |

      val fiveDataEntranceStationCodesSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "5").collect.map(row => StructField(row.getString(0), StringType))
      )
      val fiveDataEntranceStationCodesDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ";")
        .load(Paths.get(dataDir, "000005",
          "data_entrance_station_codes_v20171219_20180104_20180104.csv").toString).rdd,
        fiveDataEntranceStationCodesSchema)
      fiveDataEntranceStationCodesDF.show(3, truncate = false)
//      |LOCAL_ID|NAME                                          |LAT_WGS_84|LONG_WGS_84|STATION    |LINE                  |EVEN_DAY_HOURS                                                                                                                                               |ODD_DAY_HOURS                                                                                                                                                |N_BPA_UNIVERSAL|N_BPA_1_2_ONLY|N_BPA_TOTAL|REPAIR_INFO|GLOBAL_ID|GEO_DATA                                          |
//      +--------+----------------------------------------------+----------+-----------+-----------+----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+--------------+-----------+-----------+---------+--------------------------------------------------+
//      |326     |Китай-город, вход-выход 3 в северный вестибюль|37.6309849|55.7567672 |Китай-город|Калужско-Рижская линия|открытие в 05:30:00; закрытие в 01:00:00; первый поезд по 1 пути в 05:44:00, по 2 пути в 05:49:05; последний поезд по 1 пути в 01:35:10, по 2 пути в 01:26:20|открытие в 05:30:00; закрытие в 01:00:00; первый поезд по 1 пути в 05:43:10, по 2 пути в 05:49:50; последний поезд по 1 пути в 01:35:10, по 2 пути в 01:26:20|               |4             |4          |           |1773538  |{type=Point, coordinates=[37.6309849, 55.7567672]}|
//        |331     |Китай-город, вход-выход 8 в северный вестибюль|37.6316766|55.7573154 |Китай-город|Калужско-Рижская линия|открытие в 05:30:00; закрытие в 01:00:00; первый поезд по 1 пути в 05:44:00, по 2 пути в 05:49:05; последний поезд по 1 пути в 01:35:10, по 2 пути в 01:26:20|открытие в 05:30:00; закрытие в 01:00:00; первый поезд по 1 пути в 05:43:10, по 2 пути в 05:49:50; последний поезд по 1 пути в 01:35:10, по 2 пути в 01:26:20|               |4             |4          |           |1773539  |{type=Point, coordinates=[37.6316766, 55.7573154]}|
//        |327     |Китай-город, вход-выход 4 в северный вестибюль|37.6312698|55.7568819 |Китай-город|Калужско-Рижская линия|открытие в 05:30:00; закрытие в 01:00:00; первый поезд по 1 пути в 05:44:00, по 2 пути в 05:49:05; последний поезд по 1 пути в 01:35:10, по 2 пути в 01:26:20|открытие в 05:30:00; закрытие в 01:00:00; первый поезд по 1 пути в 05:43:10, по 2 пути в 05:49:00; последний поезд по 1 пути в 01:35:10, по 2 пути в 01:26:20|               |4             |4          |           |1773540  |{type=Point, coordinates=[37.6312698, 55.7568819]}|

      val sixDataParkingCodesSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "6").collect.map(row => StructField(row.getString(0), StringType))
      )
      val sixDataParkingCodesDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ";")
        .load(Paths.get(dataDir, "000006",
          "data_parking_codes_v20171220_20180104_20180104.csv").toString).rdd,
        sixDataParkingCodesSchema)
      sixDataParkingCodesDF.show(3, truncate = false)
//      |ID |PARKING_NAME                                                     |METRO_STATION           |GLOBAL_ID|METRO_LINE                     |ADM_AREA                              |DISTRICT             |LOCATION                                                                                      |SCHEDULE     |ORG_PHONE      |CAR_CAPACITY|LAT_WGS_84|LONG_WGS_84|GEO_DATA                                                    |
//      +---+-----------------------------------------------------------------+------------------------+---------+-------------------------------+--------------------------------------+---------------------+----------------------------------------------------------------------------------------------+-------------+---------------+------------+----------+-----------+------------------------------------------------------------+
//      |6  |Перехватывающая парковка метрополитена «Строгино»                |Строгино                |1713487  |Арбатско-Покровская линия      |Северо-Западный административный округ|район Строгино       |Строгинский бульвар, владение 14, между вестибюлями станции метро                             |круглосуточно|(495) 622-02-04|100         |37.401123 |55.803528  |{type=Point, coordinates=[37.40112300039, 55.803527999947]} |
//        |8  |Перехватывающая парковка метрополитена «Зябликово» 2             |Зябликово               |1713489  |Люблинско-Дмитровская линия    |Южный административный округ          |район Зябликово      |на пересечении Ясеневой улицы с Ореховым Бульваром, напротив пустыря с деревьями, через дорогу|круглосуточно|(495) 622-02-04|107         |37.744873 |55.612122  |{type=Point, coordinates=[37.744872999849, 55.612121999928]}|
//        |2  |Перехватывающая парковка метрополитена «Бульвар Дмитрия Донского»|Бульвар Дмитрия Донского|1713490  |Серпуховско-Тимирязевская линия|Юго-Западный административный округ   |район Северное Бутово|улица Грина, дом 5А, напротив Макдоналдс через дорогу                                         |круглосуточно|(495) 622-02-04|91          |37.57428  |55.566101  |{type=Point, coordinates=[37.574279999785, 55.566100999852]}|

      val sevenYandexLabelCodesSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "7").collect.map(row => StructField(row.getString(0), StringType))
      )
      val sevenYandexLabelCodesDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ",")
        .load(Paths.get(dataDir, "000007",
          "yandex_label_codes_20180104_20180104.csv").toString).rdd,
        sevenYandexLabelCodesSchema)
      sevenYandexLabelCodesDF.show(3, truncate = false)
//      |LABEL_ID|STATION_ID|
//      +--------+----------+
//      |1       |1         |
//        |1       |229       |
//        |2       |2         |

      val eightYandexLineCodesSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "8").collect.map(row => StructField(row.getString(0), StringType))
      )
      val eightYandexLineCodesDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ",")
        .load(Paths.get(dataDir, "000008",
          "yandex_line_codes_20180104_20180104.csv").toString).rdd,
        eightYandexLineCodesSchema)
      eightYandexLineCodesDF.show(3, truncate = false)
//      |COLOR  |LINE_ID|NAME                     |
//      +-------+-------+-------------------------+
//      |#EF1E25|1      |Сокольническая линия     |
//      |#029A55|2      |Замоскворецкая линия     |
//      |#0252A2|3      |Арбатско-Покровская линия|

      val nineYandexStationCodesSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "9").collect.map(row => StructField(row.getString(0), StringType))
      )
      val nineYandexStationCodesDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ",")
        .load(Paths.get(dataDir, "000009",
          "yandex_station_codes_20180104_20180104.csv").toString).rdd,
        nineYandexStationCodesSchema)
      nineYandexStationCodesDF.show(3, truncate = false)
//      |LABEL_ID|LINE_ID|LINK_ID|NAME                 |STATION_ID|
//      +--------+-------+-------+---------------------+----------+
//      |1       |1      |1002   |Бульвар Рокоссовского|1         |
//        |1       |1      |1229   |Бульвар Рокоссовского|1         |
//        |2       |1      |1002   |Черкизовская         |2         |

      val tenYandexLinkCodesSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "10").collect.map(row => StructField(row.getString(0), StringType))
      )
      val tenYandexLinkCodesDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ",")
        .load(Paths.get(dataDir, "000010",
          "yandex_link_codes_20180104_20180104.csv").toString).rdd,
        tenYandexLinkCodesSchema)
      tenYandexLinkCodesDF.show(3, truncate = false)
//      |FROM_STATION_ID|LINK_ID|TIME|TO_STATION_ID|TRANSFER|TRANSFER_ID|TYPE    |
//      +---------------+-------+----+-------------+--------+-----------+--------+
//      |1              |1002   |190 |2            |0       |           |link    |
//      |1              |1229   |541 |229          |1       |1229.0     |transfer|
//        |2              |2003   |145 |3            |0       |           |link    |

      val elevenYandexTransferCodesSchema = StructType(
        baseEdwexDF.select('_c3).where('_c0 === "11").collect.map(row => StructField(row.getString(0), StringType))
      )
      val elevenYandexTransferCodesDF =  spark.createDataFrame(spark.read.format("com.databricks.spark.csv")
        .option("header", "false").option("delimiter", ",")
        .load(Paths.get(dataDir, "000011",
          "yandex_transfer_codes_20180104_20180104.csv").toString).rdd,
        elevenYandexTransferCodesSchema)
      elevenYandexTransferCodesDF.show(3, truncate = false)
      val src = Map(X.EDWEX -> My(baseEdwexDF, baseEdwexDF),
        X.ITASK -> My(zeroITaskDF, baseEdwexDF.filter('_c0 === "0")),
        X.T1_METRO_GEO2 -> My(oneMetroGeoDF, baseEdwexDF.filter('_c0 === "1")),
        X.T2_MAXIMA -> My(twoMaximaDF, baseEdwexDF.filter('_c0 === "2")),
        X.T3_DATA_LINE_CODES -> My(threeDataLineCodesDF, baseEdwexDF.filter('_c0 === "3")),
        X.T4_DATA_STATION_CODES -> My(fourDataStationCodesDF, baseEdwexDF.filter('_c0 === "4")),
        X.T5_DATA_ENTRANCE_STATION_CODES -> My(fiveDataEntranceStationCodesDF, baseEdwexDF.filter('_c0 === "5")),
        X.T6_DATA_PARKING_CODES -> My(sixDataParkingCodesDF, baseEdwexDF.filter('_c0 === "6")),
        X.T7_YANDEX_LABEL_CODES -> My(sevenYandexLabelCodesDF, baseEdwexDF.filter('_c0 === "7")),
        X.T8_YANDEX_LINE_CODES -> My(eightYandexLineCodesDF, baseEdwexDF.filter('_c0 === "8")),
        X.T9_YANDEX_STATION_CODES -> My(nineYandexStationCodesDF, baseEdwexDF.filter('_c0 === "9")),
        X.T10_YANDEX_LINK_CODES -> My(tenYandexLinkCodesDF, baseEdwexDF.filter('_c0 === "10")),
        X.T11_YANDEX_TRANSFER_CODES -> My(elevenYandexTransferCodesDF, baseEdwexDF.filter('_c0 === "11")))
      val meta = exportResult(spark, Seq(), src)
      val result = etl(spark, M(src, meta.alts))
      saveResult(workDir, result.frames)
    }
    spark.sparkContext.stop
    success match { case Failure(e) => throw e case _ => }
  }

  def cache(spark: SparkSession, frame: DataFrame, edwex: DataFrame): My = {
    val edwexStr = edwex.collect.toStream.map(row => row.getString(0)
      + "\t" + row.getString(2) + "\t" + row.getString(3) + "\t" + row.getString(4)).mkString("\n")
    import org.apache.spark.sql.functions._
    val schemas = parseEDWEX(edwexStr)
    val casts = schemas.keys.toStream.sortWith(_<_).map{ index =>
      colCast(col(renameCol(index, schemas(index))), schemas(index)._2) as schemas(index)._1 }
    val res = frame.select(casts: _*)
    My(res, edwex)
  }

  def parse(spark: SparkSession, conn: Connection, sources: M): M = {
    val res = M(sources.frames.map { case (k, v) => if(k == X.EDWEX) (k,v) else (k, cache(spark, v.dat, v.edw)) },
      sources.alts)
    exportResult(spark, Seq(X.ITASK, X.T1_METRO_GEO2, X.T2_MAXIMA, X.T3_DATA_LINE_CODES, X.T4_DATA_STATION_CODES,
      X.T5_DATA_ENTRANCE_STATION_CODES, X.T6_DATA_PARKING_CODES, X.T7_YANDEX_LABEL_CODES,
      X.T8_YANDEX_LINE_CODES, X.T9_YANDEX_STATION_CODES, X.T10_YANDEX_LINK_CODES,
      X.T11_YANDEX_TRANSFER_CODES), res.frames)
  }

      /*
      val t1 = Map(X.T1_METRO_GEO2 -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T1_METRO_GEO2.toString, spark))
      val t2 = t1 + (X.T2_MAXIMA -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T2_MAXIMA.toString, spark))
      val t3 = t2 + (X.T3_DATA_LINE_CODES -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T3_DATA_LINE_CODES.toString, spark))
      val t4 = t3 + (X.T4_DATA_STATION_CODES -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T4_DATA_STATION_CODES.toString, spark))
      val t5 = t4 + (X.T5_DATA_ENTRANCE_STATION_CODES -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T5_DATA_ENTRANCE_STATION_CODES.toString, spark))
      val t6 = t5 + (X.T6_DATA_PARKING_CODES -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T6_DATA_PARKING_CODES.toString, spark))
      val t7 = t6 + (X.T7_YANDEX_LABEL_CODES -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T7_YANDEX_LABEL_CODES.toString, spark))
      val t8 = t7 + (X.T8_YANDEX_LINE_CODES -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T8_YANDEX_LINE_CODES.toString, spark))
      val t9 = t8 + (X.T9_YANDEX_STATION_CODES -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T9_YANDEX_STATION_CODES.toString, spark))
      val t10 = t9 + (X.T10_YANDEX_LINK_CODES -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T10_YANDEX_LINK_CODES.toString, spark))
      val t11 = t10 + (X.T11_YANDEX_TRANSFER_CODES -> oldDfCast("", 0, "", "", nocacheMode, "", sources(X.INFO),
        lit(1) === lit(1), X.T11_YANDEX_TRANSFER_CODES.toString, spark))
      */

  def etl(spark: SparkSession, sources: M): M = {
    import java.sql.DriverManager
    val connection = DriverManager.getConnection(sources.alts(X.EDWEX).how.getProperty("url"), sources.alts(X.EDWEX).how)

    val o0 = parse(spark, connection, sources)
    val o1 = etlO1CaseOwners(spark, connection, o0)
    val o2 = etlO2CaseRecords(spark, connection, o1)
    val o3 = etlO3CaseGraphs(spark, connection, o2)
    val o4 = etlO4CaseStats(spark, connection, o3)
    val o5 = etlO5CaseRoutes(spark, connection, o4)
    val o6 = etlO6CaseProfiles(spark, connection, o5)

    connection.close()
    o6
  }

  def etlO1CaseOwners(spark: SparkSession, conn: Connection, sources: M): M = {
    // TODO 1: ***CASE_OWNERS.CODE
    val aCaseOwnersCodeQRY = "(select 1 as troyka_wifi_matching_code) dual"
    val aCaseOwnersCodeDF = spark.read.jdbc(sources.alts(X.EDWEX).how.getProperty("url"),
      aCaseOwnersCodeQRY, sources.alts(X.EDWEX).how)



    // select cast(now() as date)
    // SELECT TO_CHAR(NOW(), 'Mon YYYY');

    M(sources.frames + (X.O1_CASE_OWNERS -> sources.frames(X.T11_YANDEX_TRANSFER_CODES)), sources.alts)
    // FIXME
  }

  def etlO2CaseRecords(spark: SparkSession, conn: Connection, sources: M): M = {
    M(sources.frames + (X.O2_CASE_RECORDS -> sources.frames(X.T11_YANDEX_TRANSFER_CODES)), sources.alts)
  }

  def etlO3CaseGraphs(spark: SparkSession, conn: Connection, sources: M): M = {
    M(sources.frames + (X.O3_CASE_GRAPHS -> sources.frames(X.T11_YANDEX_TRANSFER_CODES)), sources.alts)
  }

  def etlO4CaseStats(spark: SparkSession, conn: Connection, sources: M): M = {
    M(sources.frames + (X.O4_CASE_STATS -> sources.frames(X.T11_YANDEX_TRANSFER_CODES)), sources.alts)
  }

  def etlO5CaseRoutes(spark: SparkSession, conn: Connection, sources: M): M = {
    M(sources.frames + (X.O5_CASE_ROUTES -> sources.frames(X.T11_YANDEX_TRANSFER_CODES)), sources.alts)
  }

  def etlO6CaseProfiles(spark: SparkSession, conn: Connection, sources: M): M = {
    M(sources.frames + (X.O6_CASE_PROFILES -> sources.frames(X.T11_YANDEX_TRANSFER_CODES)), sources.alts)
  }

  def preview(chosen: DataFrame, label: String, limit: Int = 3): Unit = {
    println(Calendar.getInstance().getTime + " " + label)
    val regex = """^([a-z][A-Z].*|.*eport.*)$""".r
    if(regex.findAllIn(label).nonEmpty) {
      chosen.show(limit, truncate = false)
    }
  }

  def renameCol(index: Int, name: (String, KHD.Value)): String = name match {
    case (c, v) => c
  }

  def oldRenameCol(index: Int, name: (String, KHD.Value)): String = name match {
    case (c, v) => "c" + (index - 1) + "_" + v.toString + "_" + c
  }

  import org.apache.spark.sql.functions._
  val nullCast: (Column, KHD.FieldType) => Column = {
    (col, _) => when(col === lit("(null)"), lit(true)).otherwise(false)
  }

  val defaultMode = SaveMode.Overwrite
  val ignoredMode = SaveMode.Ignore
  val recoveryMode = SaveMode.Append
  val nocacheMode = SaveMode.ErrorIfExists
  val mode = SaveMode.ErrorIfExists
  def oldDfCast(workDir: String, exportColLimit: Long, debugInfo: String, edwex: String,
                restartMode: SaveMode, cacheDir: String,
                okDF: DataFrame, condition: Column, tableName: String, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val schemas = parseEDWEX(edwex)
    val examples = schemas.keys.toStream.sortWith(_<_).map{ index =>
      ("'_c" + (index - 1)) + "' as " + renameCol(index, schemas(index)) }.mkString(",")
    val _made = spark.sqlContext.sql("SELECT " + examples)
    preview(_made, "_" + debugInfo)  // DEBUG

    if (restartMode == recoveryMode) {
      return spark.read.parquet(cacheDir)
    }

    val paths = okDF.filter('tableName === tableName.toUpperCase)
      .select('filePath).rdd.map(_.getString(0)).collect()
    val names = schemas.keys.toStream.sortWith(_<_).map{ index =>
      col("_c" + (index - 1)) as renameCol(index, schemas(index)) }
    import org.apache.spark.sql.expressions.Window
    val made = spark.read.options(Map("header" -> "false", "quote" -> "\"", "sep" -> "\t"))
      .csv(paths: _*)
      .select(names: _*)
    // .withColumn("spark_row_number", row_number().over(Window.partitionBy(lit(1)).orderBy(lit(1))))

    preview(made, debugInfo)  // DEBUG

    val hints = schemas.keys.toStream.sortWith(_<_).map{ index =>
      col(renameCol(index, schemas.get(index).get)) as renameCol(index, schemas.get(index).get) }.toStream
    val casts = schemas.keys.toStream.sortWith(_<_).map{ index =>
      colCast(col(renameCol(index, schemas.get(index).get)), schemas.get(index).get._2) as schemas.get(index).get._1 }.toStream
    val nulls = schemas.keys.toStream.sortWith(_<_).map{ index =>
      nullCast(col(renameCol(index, schemas.get(index).get)), schemas.get(index).get._2) as schemas.get(index).get._1 + "_IS_NULL" }.toStream
    val res = made.select(hints.union(nulls).union(casts): _*)
      .withColumn("spark_row_number", row_number().over(Window.partitionBy(lit(1)).orderBy(lit(1))))
      .filter(condition)
    if (restartMode != nocacheMode) {
      res.repartition()//.coalesce(1)
        .write
        .mode(restartMode) // Ignore = skip, if _SUCCESS exist
        .parquet(cacheDir)
    }
    import org.apache.spark.storage.StorageLevel
    val r = if (restartMode != ignoredMode && restartMode != defaultMode) res else spark.read.parquet(cacheDir)
    import scala.util.{Try, Failure, Success}
    var good = r.schema.length - 1
    val success = Try {
      good = (r.schema.length - 1 to r.schema.length - 1).map(x => (x, cacheParsed(workDir, x, debugInfo, edwex,
        r.filter('spark_row_number < 100).repartition(36),
        tableName, spark))).map(_._1).max
    } match {
      case Failure(_) => cacheParsed(workDir, good, "### " + debugInfo, edwex, r.repartition(36), tableName, spark)
      case Success(_) => println("OK")
    }
    r.repartition(36).persist(StorageLevel.MEMORY_AND_DISK)
  }

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Column
  import org.apache.spark.sql.types.LongType
  val formatDate = "yyyyMMdd' 'hhmmss.SSSSSS"
  val formatTs = "yyyyMMdd' 'hhmmss.SSSSSS"
  val colCast: (Column, KHD.FieldType) => Column = {
    (col, kind) => {
      (col, kind) match {
        case (c, KHD.NUMERIC) => when(c === lit("null"), lit(0L)).otherwise(c cast LongType)
        case (c, KHD.NUMBER155) => when(c === lit("null"), lit(0L)).otherwise(c cast LongType)
        case (c, KHD.NUMBER53) => when(c === lit("null"), lit(0L)).otherwise(c cast LongType)
        case (c, KHD.NUMBER18) => when(c === lit("null"), lit(0L)).otherwise(c cast LongType)
        case (c, KHD.NUMBER12) => when(c === lit("null"), lit(0L)).otherwise(c cast LongType)
        case (c, KHD.NUMBER10) => when(c === lit("null"), lit(0L)).otherwise(c cast LongType)
        case (c, KHD.NUMBER3) => when(c === lit("null"), lit(0L)).otherwise(c cast LongType)
        case (c, KHD.DATE) => when(c === lit("null"), lit(0L)).otherwise(unix_timestamp(c, formatDate))
        case (c, KHD.TIMESTAMP) => when(c === lit("null"), lit(0L)).otherwise(unix_timestamp(c, formatTs))
        case (c, _) => c
      }
    }
  }

  def hiveFields(exportColLimit: Long, dict: Map[Int, (String, KHD.FieldType)]): String = {
    //        + "`primary_id` INT COMMENT 'primary_id of index', "
    //        + "`secondary_id` INT COMMENT 'secondary_id of index', "
    //        + "`index_name` STRING COMMENT 'name of index', "
    //        + "`data_type` STRING COMMENT 'data type of index', "
    //        + "`is_general` INT COMMENT '1-general; 0-for each month' "

    // READ a/26512415/7713755 HOW TO INSERT TIMESTAMP
    val s1 = dict.keys.toStream.sortWith(_<_).map(k => (k, (dict.get(k).get._1, dict.get(k).get._2))).map{
      case (a, b) => {
        val re = renameCol(a, b)
        (b._1, b._2) match {
          case (a, KHD.NUMERIC) => "`" + re + "` INT COMMENT 'primary_id of index'"
          case (a, KHD.NUMBER155) => "`" + re + "` INT COMMENT 'primary_id of index'"
          case (a, KHD.NUMBER53) => "`" + re + "` INT COMMENT 'primary_id of index'"
          case (a, KHD.NUMBER18) => "`" + re + "` INT COMMENT 'primary_id of index'"
          case (a, KHD.NUMBER12) => "`" + re + "` INT COMMENT 'primary_id of index'"
          case (a, KHD.NUMBER10) => "`" + re + "` INT COMMENT 'primary_id of index'"
          case (a, KHD.NUMBER3) => "`" + re + "` INT COMMENT 'primary_id of index'"
          case (a, KHD.DATE) => "`" + re + "` DATE COMMENT 'primary_id of index'"
          case (a, KHD.TIMESTAMP) => "`" + re + "` TIMESTAMP COMMENT 'primary_id of index'"
          case (a, _) => "`" + re + "` STRING COMMENT 'primary_id of index'"
        }
      }}.take(exportColLimit.toInt)
    val s2 = dict.keys.toStream.sortWith(_<_).map(k => {
      dict.get(k).get._2 match {
        case _ => "`" + dict.get(k).get._1 + "_IS_NULL` BOOLEAN COMMENT 'primary_id of index'"
      }
    }).take(exportColLimit.toInt)
    val s3 = dict.keys.toStream.sortWith(_<_).map(k => {
      dict.get(k).get._2 match {
        case KHD.NUMERIC => "`" + dict.get(k).get._1 + "` INT COMMENT 'primary_id of index'"
        case KHD.NUMBER155 => "`" + dict.get(k).get._1 + "` INT COMMENT 'primary_id of index'"
        case KHD.NUMBER53 => "`" + dict.get(k).get._1 + "` INT COMMENT 'primary_id of index'"
        case KHD.NUMBER18 => "`" + dict.get(k).get._1 + "` INT COMMENT 'primary_id of index'"
        case KHD.NUMBER12 => "`" + dict.get(k).get._1 + "` INT COMMENT 'primary_id of index'"
        case KHD.NUMBER10 => "`" + dict.get(k).get._1 + "` INT COMMENT 'primary_id of index'"
        case KHD.NUMBER3 => "`" + dict.get(k).get._1 + "` INT COMMENT 'primary_id of index'"
        case KHD.DATE => "`" + dict.get(k).get._1 + "` DATE COMMENT 'primary_id of index'"
        case KHD.TIMESTAMP => "`" + dict.get(k).get._1 + "` TIMESTAMP COMMENT 'primary_id of index'"
        case _ => "`" + dict.get(k).get._1 + "` STRING COMMENT 'primary_id of index'"
      }
    }).take(exportColLimit.toInt)
    val s4 = Seq("`spark_row_number` INT COMMENT 'primary_id of index'")
    s1.union(s2).union(s3).take(exportColLimit.toInt).union(s4).mkString(", ")
  }

  object KHD extends Enumeration {
    type FieldType = Value
    val VARCHAR65535, VARCHAR4000, VARCHAR2000, VARCHAR255, VARCHAR250, VARCHAR150,
    VARCHAR64, VARCHAR30, VARCHAR15, VARCHAR2,
    CHAR1, NUMERIC, NUMBER155, NUMBER53, NUMBER18, NUMBER12, NUMBER10, NUMBER3,
    DATE, TIMESTAMP = Value
  }

  def parseKind(value: String, precision: String): KHD.FieldType = {
    val up = value.toUpperCase()
    val str = up + precision.toUpperCase()
    for (d <- KHD.values) if ("" + d == str) return d
    import scala.util.Try
    Try {
      val p = precision.toLong
      for (x <- p until 65536) for (d <- KHD.values) if ("" + d == up + x) return d
    }
    throw new Exception("UNKNOWN KIND: " + str)
  }

  def parseEDWEX(str: String): Map[Int, (String, KHD.FieldType)] = {
    val lst = str.stripMargin.toLowerCase.split("\n").map(_.replaceAll("/", "_")
      .replaceAll("\\$", "s").replaceAll("#", "h")
      .replaceAll("[^a-zA-Z0-9_]", " ").replaceAll("[ ][ ]*", " ")
      .replaceAll("^[ ]*", "").replaceAll("[ ]*$", "")).filterNot(_.isEmpty)
    val a = lst.map{ v => {
      val fields = v.split(" ")
      val sz = fields.length
      val info = if (sz > 0) fields(0) else ""
      val name = if (sz > 2) fields(2) else ""
      val value = if (sz > 3) fields(3) else ""
      val precision = if (sz > 4) fields(4) else ""
      (info, name, value, precision)
    } }.filter{case(_, name, _, _) => name.nonEmpty}
      .zipWithIndex.map{ case((info, name, value, precision), i) => {
      val kind = if (info != "__") parseKind(value, precision) else KHD.VARCHAR65535
      (i + 1, (info, name, kind))
    } }.filter{case(id, (info, _, _)) => info != "__"}
      .map{case(id, (_, name, kind)) => (id, (name, kind))}
    a.toMap
  }

  def cacheParsed(workDir: String, exportColLimit: Long, debugInfo: String, edwex: String,
                  dataDF: DataFrame, tableName: String, spark: SparkSession): DataFrame = {
    // READ https://git.restr.im/bigdata/dmp/blob/dev/datashowcase_builder/src/main/java
    // /ru/rt/restream/iptv/datashowcase/builder/job/LoadDictTvTheme.java
    val hiveFullSchemaName = "rtk_dm_bdt21"
    spark.sql("CREATE SCHEMA IF NOT EXISTS " + hiveFullSchemaName)
    val hiveFullTableName = hiveFullSchemaName + "." + tableName
    spark.sql("DROP TABLE IF EXISTS " + hiveFullTableName)
    val tableLocation = workDir + "/x0_" + tableName
    val query = "CREATE EXTERNAL TABLE " + hiveFullTableName + " ( " + hiveFields(exportColLimit, parseEDWEX(edwex)) +
      ") " + "COMMENT 'The table is dictionary of index names ver. " + "1.0" + "' " +
      "STORED AS AVRO " + "LOCATION '" + tableLocation + "'"
    //        + "`primary_id` INT COMMENT 'primary_id of index', "
    //        + "`secondary_id` INT COMMENT 'secondary_id of index', "
    //        + "`index_name` STRING COMMENT 'name of index', "
    //        + "`data_type` STRING COMMENT 'data type of index', "
    //        + "`is_general` INT COMMENT '1-general; 0-for each month' "
    spark.sql(query)
    val schema: Seq[StructField] = dataDF.schema.slice(1, exportColLimit.toInt + 1)
    val names = schema.map(sf => sf.name).union(Seq("spark_row_number"))
    println(debugInfo + " query " + query )
    println(debugInfo + " export " + exportColLimit + " " + names.mkString(", ") )
    dataDF.select(names.map(col): _*).write
      .option("path", tableLocation)
      .mode(SaveMode.Overwrite)
      .insertInto(hiveFullTableName)
    dataDF
  }

  def saveResult(workDir: String, result: Map[XType, My]): Unit = {
    result(X.T1_METRO_GEO2).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T1_METRO_GEO2.toString).toString)

    result(X.T2_MAXIMA).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T2_MAXIMA.toString).toString)

    result(X.T3_DATA_LINE_CODES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T3_DATA_LINE_CODES.toString).toString)

    result(X.T4_DATA_STATION_CODES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T4_DATA_STATION_CODES.toString).toString)

    result(X.T5_DATA_ENTRANCE_STATION_CODES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T5_DATA_ENTRANCE_STATION_CODES.toString).toString)

    result(X.T6_DATA_PARKING_CODES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T6_DATA_PARKING_CODES.toString).toString)

    result(X.T7_YANDEX_LABEL_CODES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T7_YANDEX_LABEL_CODES.toString).toString)

    result(X.T8_YANDEX_LINE_CODES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T8_YANDEX_LINE_CODES.toString).toString)

    result(X.T9_YANDEX_STATION_CODES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T9_YANDEX_STATION_CODES.toString).toString)

    result(X.T10_YANDEX_LINK_CODES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T10_YANDEX_LINK_CODES.toString).toString)

    result(X.T11_YANDEX_TRANSFER_CODES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.T11_YANDEX_TRANSFER_CODES.toString).toString)

    result(X.O1_CASE_OWNERS).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.O1_CASE_OWNERS.toString).toString)

    result(X.O2_CASE_RECORDS).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.O2_CASE_RECORDS.toString).toString)

    result(X.O3_CASE_GRAPHS).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.O3_CASE_GRAPHS.toString).toString)

    result(X.O4_CASE_STATS).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.O4_CASE_STATS.toString).toString)

    result(X.O5_CASE_ROUTES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.O5_CASE_ROUTES.toString).toString)

    result(X.O6_CASE_PROFILES).dat.coalesce(1).write
      .options(Map("header" -> "true", "sep" -> ";", "quoteAll" -> "true", "compression" -> "gzip"))
      .mode(SaveMode.Overwrite).csv(Paths.get(workDir, X.O6_CASE_PROFILES.toString).toString)
  }

  import java.util.Properties
  def exportResult(spark: SparkSession, flt: Seq[X.XType], result: Map[XType, My]): M = {
    // https://gist.github.com/brock/7a7a70300096632cec30
    // READ https://docs.databricks.com/spark/latest/data-sources/sql-databases.html

    val jUsername = "postgres"
    val jPassword = "123456"
    val jHostname = "127.0.0.1"
    val jPort = 5432
    val jDatabase =""
    val jUrl = s"jdbc:postgresql://${jHostname}:${jPort}/${jDatabase}?user=${jUsername}&password=${jPassword}"
    val connectionProperties = new Properties()
    connectionProperties.put("user", jUsername)
    connectionProperties.put("password", jPassword)
    connectionProperties.put("url", jUrl)

    import java.sql.DriverManager
    val connection = DriverManager.getConnection(connectionProperties.getProperty("url"), connectionProperties)

    val eSeq = Seq(X.EDWEX)

    val tSeq = Seq(X.ITASK, X.T1_METRO_GEO2, X.T2_MAXIMA, X.T3_DATA_LINE_CODES, X.T4_DATA_STATION_CODES,
      X.T5_DATA_ENTRANCE_STATION_CODES, X.T6_DATA_PARKING_CODES, X.T7_YANDEX_LABEL_CODES,
      X.T8_YANDEX_LINE_CODES, X.T9_YANDEX_STATION_CODES, X.T10_YANDEX_LINK_CODES, X.T11_YANDEX_TRANSFER_CODES)

    val oSeq = Seq(X.O1_CASE_OWNERS, X.O2_CASE_RECORDS, X.O3_CASE_GRAPHS,
      X.O4_CASE_STATS, X.O5_CASE_ROUTES, X.O6_CASE_PROFILES)

    eSeq.union(tSeq.union(tSeq)).filter(flt.union(Seq(X.EDWEX)).contains(_)).map(xt => {
      result(xt).dat.createOrReplaceTempView(xt.toString)
      spark.table(xt.toString).write.mode(SaveMode.Overwrite)
        .jdbc(jUrl, "v001_" + xt.toString, connectionProperties)
      true
    })
    connection.close()
    M(result, result.map{case(k, _) => (k, Db("v001_" + k.toString, connectionProperties))})
  }
}
