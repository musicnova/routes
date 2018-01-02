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
    println("Welcome to Spark!")
    initLog4j()
    lazy val spark: SparkSession = SparkSession.builder().appName("admin_MOS-0_schema_N1")
      //.config("spark.sql.warehouse.dir", "/tmp_spark/spark-warehouse")
      //.config("spark.local.dir", "/tmp_spark/spark-temp")
      .config("spark.driver.maxResultSize", "300g")
      //.config("spark.worker.cleanup.enabled", "true")
      //.config("spark.worker.cleanup.interval", "900")
      .getOrCreate
      // .master("local[*]").getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // %spark

    // In[3]
    val dataDir = "/home/user/DEPTRANS"

    // In[4]
    import java.nio.file.Paths
    val dfInetGeo2Dicts = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(Paths.get(dataDir, "temp_inet", "metro_geo2.csv").toString)
    dfInetGeo2Dicts.show(3, truncate = false)

    // In[5]
    val dfTroykaLines = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(Paths.get(dataDir, "temp_troyka", "line_codes_csv.csv").toString)
    dfTroykaLines.show(3, truncate = false)

    // In[6]
    val dfTroykaStations = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(Paths.get(dataDir, "temp_troyka", "station_codes_csv.csv").toString)
    dfTroykaLines.show(3, truncate = false)

    // In[7]
    val dfTroykaEntranceStations = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(Paths.get(dataDir, "temp_troyka", "entrance_station_codes_csv.csv").toString)
    dfTroykaLines.show(3, truncate = false)

    // In[8]


    // TODO: spark.sparkContext.stop
    // success match { case Failure(e) => throw e case _ => }
  }
}
