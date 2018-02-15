package ideas

import java.sql.Date

import ideas.UtilWifi0Dicts.{Dict, TableDict}
import ideas.UtilWifi0Errors.RecError
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object UtilWifi0Loads extends Serializable {
  case class RecLoad(id: Option[Int], casePart: String, datePart: Date, ts: Long, what: String, vol: Long, del: Long)
  case class StatKey(area: Double, step: Long, x0: Long, x1: Long, mints: Long, maxts: Long)
  case class GridKey(table: String, ts: Long)
  case class GridValue(rec: RecError, st: StatKey)

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
    val df = dfNightly.select(unix_timestamp('when, "dd-MM-yyyy HH:mm:ss") cast TimestampType cast LongType,
      'what, 'total cast LongType, unix_timestamp('stamp, "dd-MM-yyyy HH:mm:ss") cast TimestampType cast LongType)

    saveLoadsToDB(readLoads(df), TableLoad.table)
  }

  case class Load(id: Option[Int], stepType: Int,
                  ts: Long, what: Int, total: Long, stamp: Long, trash: Int, deleted: Long)

  import slick.jdbc.PostgresProfile.api.{DBIO, Database, Table, TableQuery, Tag}
  import slick.jdbc.PostgresProfile.api._


  class TableLoad(tag: Tag) extends Table[Load](tag, "wifi0_load") {
    def id = column[Option[Int]]("id", O.PrimaryKey, O.AutoInc)
    def stepType = column[Int]("step_type")
    def ts = column[Long]("ts")
    def what = column[Int]("what")
    def total = column[Long]("total")
    def stamp = column[Long]("stamp")
    def trash = column[Int]("trash")
    def deleted = column[Long]("deleted")
    override def * = (id, stepType, ts, what, total, stamp, trash, deleted) <> (Load.tupled, Load.unapply)
  }

  object TableLoad {
    def table = TableQuery[TableLoad]
  }

  def readLoads(df: DataFrame): Stream[Load] = {
    val stepType = 1
    df.rdd.map(row => Load(None, stepType, row.getLong(0),
      row.getString(1) match {
        case "maxima_data.metro" => 1
        case "metro_data.entries" => 2
        case "codd_data.codd" => 3
        case _ => 0
    }, row.getLong(2), row.getLong(3), 0, 0)).collect.toStream
  }

  import scala.concurrent.Await
  import scala.concurrent.duration.Duration
  import scala.concurrent.ExecutionContext.Implicits.global // for future processing
  def saveLoadsToDB(data: Stream[Load],
                    tableQuery: TableQuery[TableLoad],
                    db: Database = Database.forConfig("db.default")): Unit = try {
    val actions = data.map { dcs =>
      tableQuery.filter(cr =>
          cr.stepType === dcs.stepType &&
          cr.ts === dcs.ts &&
          cr.what === dcs.what &&
          cr.stamp === dcs.stamp
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
