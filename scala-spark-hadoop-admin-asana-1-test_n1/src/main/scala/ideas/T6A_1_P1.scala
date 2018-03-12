package ideas

import org.apache.spark.sql.SparkSession

object T6A_1_P1 extends Serializable {

  def initLog4j(): Unit = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    println("Welcome to Spark!")
    initLog4j()
    lazy val spark: SparkSession = SparkSession.builder().appName("admin_asana-1_test_N1")
      //.config("spark.sql.warehouse.dir", "/tmp_spark/spark-warehouse")
      //.config("spark.local.dir", "/tmp_spark/spark-temp")
      .config("spark.driver.maxResultSize", "300g")
      //.config("spark.worker.cleanup.enabled", "true")
      //.config("spark.worker.cleanup.interval", "900")
      // .getOrCreate
      .master("local[*]").getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // %spark

    // 1. Для каждого уникального идентификатора чипа билета (TICKET_UID) из таблицы METRO_DATA.ENTRIES формируем 1 запись следующим образом:
    // 1.1. Отсекаем по дате и времени валидации (POINT_TIME) записи за расчетный период от @DATA_DEPTH_1 до @DATA_DEPTH_2 .
    // 1.2. Сортируем записи по возрастанию даты и времени валидации (POINT_TIME) в рамках каждых суток.
    // 1.3. На основе сортировки определяем атрибуты согласно маппингу на вкладке CALC Атрибуты и следующим правилам:
    // 1.3.1. Атрибуты TICKET_UID, TICKET_NUM, TICKET_TYPE, ENTER_DTTM заполняем 1-в-1 значениями атрибутов таблицы METRO_DATA.ENTRIES.
    // 1.3.2. Атрибуты STATION_ID, STATION_NM, LINE_ID, LINE_NM, ENTRANCE_ID, ENTRANCE_NM определяем из справочника METRO_CALC.D_SUBWAY_ENTRANCE через условие METRO_DATA.ENTRIES.ENTRANCE_ID = METRO_CALC.D_SUBWAY_ENTRANCE.ENTRANCE_ID.
    // 1.3.3. Атрибуты ENTER_DT, WEEK_NUM, ENTER_DAY_OF_WEEK, ENTER_TIME определяем либо напрямую из даты и времени валидации (POINT_TIME), либо из справочника DICT.D_DATE через условие DICT.D_DATE.DATE_DATE = дате из METRO_DATA.ENTRIES.POINT_TIME.
    // 1.3.3.1. При этом атрибут WEEK_NUM заполняется порядковым номером недели относительно @DATA_DEPTH_1, т.е. если @DATA_DEPTH_1 приходится на среду, то первой неделей считаются 7 дней со среды этой недели по вторник следующей недели включительно.
    // 1.3.4. Атрибут ENTER_LAG расчитываем как разность между временем текущей и предыдущей валидации (POINT_TIME), выраженную в часах (пример: время текущей валидации POINT_TIME = '2018-02-06 18:58:33', время предыдущей валидации POINT_TIME = '2018-02-06 18:44:58' -> ENTER_LAG = 0,21).
    // 1.3.5. Атрибут ENTER_NUM заполняем как порядковый номер валидации в рамках суток (от 00:00 до 23:59 одного календаря дня).
    // 1.3.6. Атрибут LAST_ENTER_FLG заполняем значением '1' в случае, если текущая валидация является последней для UID в рамках календарных суток (от 00:00 до 23:59), иначе заполняется значением '0'.
    // 2. Сформированные записи помещаем в таблицу METRO_CALC.F_SUBWAY_STATION_VISIT.

    // 1) METRO_DATA.ENTRIES
    // 2) METRO_CALC.D_SUBWAY_ENTRANCE
    // 1) METRO_CALC.F_SUBWAY_STATION_VISIT

    // Тестовые сценарии: Алгоритм 6

    // Входные таблицы:

    // 1) METRO_DATA.ENTRIES
    //  |entries.point_time   |entries.entrance_id|entries.ticket_number|entries.uid          |entries.ticket_type|entries.entry_type|entries.ride_count|entries.rides_left|entries.hash_uid                |entries.day|
    //  +---------------------+-------------------+---------------------+---------------------+-------------------+------------------+------------------+------------------+--------------------------------+-----------+
    //  |2015-01-01 05:27:09.0|220                |9230930              |930118934            |1509               |0                 |726               |0                 |9e28caf29ea8bbb9a1c35688540e9649|2015-01-01 |
    //  |2015-01-01 05:27:04.0|27                 |1011866424           |1234097026512258     |4388               |0                 |0                 |0                 |fe555af489497aadbe49b72f7af0b99f|2015-01-01 |
    //  |2015-01-01 05:27:38.0|272                |2259077700           |1,46959488288229E+016|4371               |0                 |0                 |0                 |581cc6dc143de58db2320ee5d22262b5|2015-01-01 |


    // 2) METRO_CALC.D_METRO_ENTRANCE
    // |d_metro_entrance.entrance_id|d_metro_entrance.entrance_nm    |d_metro_entrance.station_id|d_metro_entrance.station_nm|d_metro_entrance.line_id|d_metro_entrance.line_nm|
    // +----------------------------+--------------------------------+---------------------------+---------------------------+------------------------+------------------------+
    // |20                          |Бульвар Рокоссовского (Северный)|19                         |Бульвар Рокоссовского      |8                       |СОКОЛЬНИЧЕСКАЯ ЛИНИЯ    |
    // |21                          |Бульвар Рокоссовского ( Южный ) |19                         |Бульвар Рокоссовского      |8                       |СОКОЛЬНИЧЕСКАЯ ЛИНИЯ    |
    // |23                          |Черкизовская ( Северный )       |22                         |Черкизовская               |8                       |СОКОЛЬНИЧЕСКАЯ ЛИНИЯ    |


    // 3) DICT.D_DATE
    // +----------------+----------------+-----------------+---------------+------------------+------------------------+---------------------+---------------------------+
    // |d_date.date_date|d_date.date_year|d_date.date_month|d_date.date_day|d_date.date_string|d_date.date_string_short|d_date.date_dayofweek|d_date.date_dayofweek_short|
    // |2015-01-01      |2015            |1                |1              |1 январь 2015     |1 янв 2015              |четверг              |чт                         |
    // |2015-01-02      |2015            |1                |2              |2 январь 2015     |2 янв 2015              |пятница              |пт                         |
    // |2015-01-03      |2015            |1                |3              |3 январь 2015     |3 янв 2015              |суббота              |сб                         |


    // Выходные таблицы:

    // 1) METRO_CALC.F_SUBWAY_STATION_VISIT

    // По каждому пункту T_1.3.1_P1.docx - позитивный тест

    // 1) Описание как у Даши

    // 2) CREATE TABLE AS SELECT для отбора данных в таблицу

    // 3) CREATE TABLE AS SELECT для expected результата

    // 4) CREATE TABLE AS SELECT для проверки правильности

    // >>> Даты 8 недель - это ноябрь и декабрь 2017 <<<

    // === T6A_1_P1
    import org.apache.spark._;
    import org.apache.spark.sql._;
    import spark.implicits._;
    import org.apache.spark.sql.functions._;
    import org.apache.spark.sql.expressions.Window;
    import org.apache.spark.sql.types._;

    // Original 1. Для каждого уникального идентификатора чипа билета (TICKET_UID) из таблицы METRO_DATA.ENTRIES
    // формируем 1 запись следующим образом:

    // Task: Из ENTRIES: CREATE TABLE AS SELECT для отбора данных в таблицу
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dfInput = hiveContext.sql("SELECT ticket_number, hash_uid" +
      " FROM maxima_data_entries WHERE hash_uid = '9e28caf29ea8bbb9a1c35688540e9649'")

    dfInput.show(3, truncate=false)

    val dfResult = dfInput
    dfResult.show(3, truncate=false)

    hiveContext.sql("drop table if exists sandbox.yurbasov_t6a_1_p1")
    dfResult.write.saveAsTable("sandbox.yurbasov_t6a_1_p1")

    // TODO: spark.sparkContext.stop
    // success match { case Failure(e) => throw e case _ => }
  }
}
