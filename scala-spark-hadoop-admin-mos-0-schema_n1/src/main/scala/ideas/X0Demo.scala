package ideas

import org.apache.spark.sql.{SparkSession, Column}
import org.apache.spark.sql.types.StructField

object X0Demo extends Serializable {
  def initLog4j(): Unit = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    println("Welcome to Spark!")
    initLog4j()
    val wareDir = "/tmp/tmp_spark/spark-warehouse"
    val tempDir = "/tmp/tmp_spark/spark-temp"
    val workDir = "/tmp/tmp_spark/spark-temp/x0demo"
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
      val testCondition: Column = /*'spark_row_number < */lit(1000) === lit(1000)
    }
    spark.sparkContext.stop
    success match { case Failure(e) => throw e case _ => }
  }
}
