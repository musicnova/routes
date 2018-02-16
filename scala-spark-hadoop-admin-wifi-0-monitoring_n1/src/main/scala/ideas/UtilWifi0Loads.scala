package ideas

import java.sql.Date
import java.text.SimpleDateFormat

import ideas.UtilWifi0Dicts.{Dict, TableDict}
import ideas.UtilWifi0Errors.RecError
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object UtilWifi0Loads extends Serializable {

  def initLog4j(): Unit = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    println("Welcome to Spark! Local")
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
    val workDir = if (args.length < 1) "/home/user/ALBINA/YURIY/NIGHTLY/" else args(0)
    val dfNightly = spark.read.option("sep", ",").csv("/home/user/ALBINA/YURIY/NIGHTLY")
      .select('_c0 as "when", '_c1 as "what", '_c2 as "total", '_c3 as "stamp")
      .filter(!'when.startsWith("yurbasov_"))

    dfNightly.show()

    import org.apache.spark.sql.functions.unix_timestamp
    val df = dfNightly.select(unix_timestamp('when, "yyyy-MM-dd HH:mm:ss") cast TimestampType,
      'what, 'total cast LongType, unix_timestamp('stamp, "yyyy-MM-dd HH:mm:ss") cast TimestampType)

    df.show()
    saveLoadsToDB(readLoads(df), TableLoad.table)
  }

  case class Load(id: Option[Int], vStep: Int,
                  vTs: Long, vWhat: Int, vTotal: Long, vStamp: Long, vTrash: Int, vDeleted: Long)

  import slick.jdbc.PostgresProfile.api.{DBIO, Database, Table, TableQuery, Tag}
  import slick.jdbc.PostgresProfile.api._

  class TableLoad(tag: Tag) extends Table[Load](tag, "wifi0_load") {
    def id = column[Option[Int]]("id", O.PrimaryKey, O.AutoInc)
    def vStep = column[Int]("v_step")
    def vWhen = column[Long]("v_when")
    def vWhat = column[Int]("v_what")
    def vTotal = column[Long]("v_total")
    def vStamp = column[Long]("v_stamp")
    def vTrash = column[Int]("v_trash")
    def vDeleted = column[Long]("v_deleted")
    override def * = (id, vStep, vWhen, vWhat, vTotal, vStamp, vTrash, vDeleted) <> (Load.tupled, Load.unapply)
  }

  object TableLoad {
    def table = TableQuery[TableLoad]
  }

  def readLoads(df: DataFrame): Stream[Load] = {
    val stepType = 1
    val res = df.rdd.map(row => {
      val dtWhen = row.getTimestamp(0)
      val dtStamp = row.getTimestamp(3)
      // println(dtWhen)
      // println(dtStamp)
      val when = dtWhen.getTime / 1000

      val format = new SimpleDateFormat("yyyyMMdd")
      val stamp = format.format(dtStamp.getTime).toLong
      // println(when)
      // println(stamp)
      Load(None, stepType, when,
      row.getString(1) match {
        case "maxima_data.metro" => 40
        case "metro_data.entries" => 29
        case "codd_data.codd" => 54
        case _ => 0
      }, row.getLong(2), stamp, 0, 0) })
    res.collect.toStream
  }

  import scala.concurrent.Await
  import scala.concurrent.duration.Duration
  import scala.concurrent.ExecutionContext.Implicits.global // for future processing
  def saveLoadsToDB(data: Stream[Load],
                    tableQuery: TableQuery[TableLoad],
                    db: Database = Database.forConfig("db.default")): Unit = try {
    val actions = data.map { dcs =>
      tableQuery.filter(cr =>
          cr.vStep === dcs.vStep &&
          cr.vWhen === dcs.vTs &&
          cr.vWhat === dcs.vWhat &&
          cr.vStamp === dcs.vStamp
      ).exists.result.flatMap {
        case false => tableQuery += dcs
        case true => DBIO.successful()
      }
    }
    Await.result(
      db.run(DBIO.seq(actions: _*).transactionally)
      , Duration.Inf)
  } finally db.close
}
