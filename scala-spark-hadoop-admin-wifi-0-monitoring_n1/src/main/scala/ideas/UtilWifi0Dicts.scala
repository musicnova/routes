package ideas

import java.sql.Date

import ideas.Main.initLog4j
import org.apache.spark.sql.SparkSession

object UtilWifi0Dicts extends Serializable {
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
    saveDictsToDB(readDict(), TableDict.table)
  }

  case class Dict(id: Option[Int], objectType: Int, key: String, value: String, deleted: Long)

  import slick.jdbc.PostgresProfile.api.{DBIO, Database, Table, TableQuery, Tag}
  import slick.jdbc.PostgresProfile.api._


  class TableDict(tag: Tag) extends Table[Dict](tag, "wifi0_dict") {
    def id = column[Option[Int]]("id", O.PrimaryKey, O.AutoInc)
    def objectType = column[Int]("object_type")
    def key = column[String]("key")
    def value = column[String]("value")
    def deleted = column[Long]("deleted")
    override def * = (id, objectType, key, value, deleted) <> (Dict.tupled, Dict.unapply)
  }

  object TableDict {
    def table = TableQuery[TableDict]
  }

  def readDict(): Stream[Dict] = {
    val objectType = 1 // "TABLE_NAME"
    Map("metro_data.entries" -> "Fakty wifi", "maxima_data.metro" -> "Fakty metro")
      .map{case (k,v) => Dict(None, objectType, k, v, 0)}.toStream
      .union(
        Map("codd_data.codd" -> "Fakty extra")
          .map{case (k,v) => Dict(None, objectType, k, v, 0)}.toStream)
  }

  import scala.concurrent.Await
  import scala.concurrent.duration.Duration
  import scala.concurrent.ExecutionContext.Implicits.global // for future processing
  def saveDictsToDB(data: Stream[Dict],
                    tableQuery: TableQuery[TableDict],
                    db: Database = Database.forConfig("db.default")): Unit = try {
    val actions = data.map { dcs =>
      tableQuery.filter(cr =>
        cr.objectType === dcs.objectType &&
          cr.key === dcs.key &&
          cr.value === dcs.value
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
