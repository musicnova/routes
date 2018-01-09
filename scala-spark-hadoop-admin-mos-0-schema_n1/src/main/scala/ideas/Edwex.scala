package ideas

import java.io.{File, FileWriter, PrintWriter}

object Edwex extends Serializable {
  def using[A <: {def close(): Unit}, B](resource: A)(f: A => B): B =
    try f(resource) finally resource.close()

  def writeToFile(path: String, data: String): Unit =
    using(new FileWriter(path))(_.write(data))

  def appendToFile(path: String, data: String): Unit =
    using(new PrintWriter(new FileWriter(path, true)))(_.println(data))

  def accessAll(): String = {
    import com.github.tototoshi.csv.DefaultCSVFormat
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = '\t'
      override val lineTerminator = "\n"
    }

    val runcmd = "=CONCATENATE(\"run -srctp=001 -srcno=\";D5;\" -specver=C " +
      "-dbcs=jdbc:oracleI5:thin:@<имя/IP-адрес сервера БД>:<port>:<alias> -dbuser=<имя>" +
      " -dbpass=<пароль>\";\" -svncs=http://<имя/IP-адрес SVN-сервера> -svnuser=<имя>" +
      " -svnpass=<пароль> -encoding=utf-8 -depers=1 -maxthreads=2 -disabledCryptoPro=1 -tgtpath=\";N5)"
    // https://github.com/tototoshi/scala-csv
    // https://www.programcreek.com/scala/java.io.ByteArrayOutputStream
    import com.github.tototoshi.csv.CSVWriter
    import java.io.ByteArrayOutputStream
    val stream = new ByteArrayOutputStream()
    val writer = CSVWriter.open(stream)
    writer.writeRow(Seq("Поле 1", "Поле 2", "Поле 3", "Поле 4"))
    writer.writeRow(Seq("Система источник данных"))
    writer.writeRow(Seq("src_system_key", "src_system_name", "src_system_desc", "branch_name", "src_system_type"))
    writer.writeRow(Seq("(1) Идентификатор системы источника данных",
    "(2) Наименование систем-источника данных", "(3) Описание системы-источника данных", "(4) Наименование региона",
    "(5) Тип системы-источника", "(6) IP адрес и порт сервера для доступа по SSH/PowerShell",
    "(7) Логин для запуска выгрузки", "(8) Командная строка запуска выгрузки",
    "(9) Адрес FTP сервера", "(10) Логин для FTP сервера", "(11) Путь к данным", "(12) ФИО, email администратора",
    "(13) Общий объем таблиц, содержащих персональные данные, Гб",
    "(14) Объем ежемесячного прироста таблиц, содержащих персональные данные, Гб", "(15) Окно регулярной выгрузки",
    "(16) Окно историчной выгрузки", "(17) Окно забора данных по FTP историчной выгрузки", "(18) Комментарий"))
    // 82.202.228.187:2207 sftpclient3 \ city-1637
    writer.writeRow(Seq("0", "www", "НСИ", "Москва", "1",  "", "", "#metro_geo2", "", "", "", "", "", "", "", "", "", ""))
    writer.writeRow(Seq("1", "maximatelecom", "НСИ", "Москва", "1", "", "", "#maxima_20170615", "", "", "", "", "", "", "", "", "", ""))
    writer.writeRow(Seq("2", "data", "НСИ", "Москва", "1",  "", "", "#data_line_codes_20171224", "", "", "", "", "", "", "", "", "", "https://data.mos.ru/classifier/7704786030-linii-moskovskogo-metropolitena"))
    writer.writeRow(Seq("3", "data", "НСИ", "Москва", "1",  "", "", "#data_station_codes_20171224", "", "", "", "", "", "", "", "", "", "https://data.mos.ru/classifier/7704786030-stantsii-moskovskogo-metropolitena"))
    writer.writeRow(Seq("4", "data", "НСИ", "Москва", "1",  "", "", "#data_entrance_station_codes_20171219", "", "", "", "", "", "", "", "", "", "https://data.mos.ru/opendata/7704786030-vhody-i-vyhody-vestibyuley-stantsiy-moskovskogo-metropolitena"))
    writer.writeRow(Seq("5", "data", "НСИ", "Москва", "1",  "", "", "#data_parking_codes_20171220", "", "", "", "", "", "", "", "", "", "https://data.mos.ru/opendata/7704786030-perehvatyvayushchie-parkovki"))
    writer.writeRow(Seq("6", "yandex", "НСИ", "Москва", "1",  "", "", "#yandex_label_codes_csv", "", "", "", "", "", "", "", "", "", ""))
    writer.writeRow(Seq("7", "yandex", "НСИ", "Москва", "1",  "", "", "#yandex_line_codes_csv", "", "", "", "", "", "", "", "", "", ""))
    writer.writeRow(Seq("8", "yandex", "НСИ", "Москва", "1",  "", "", "#yandex_station_codes_csv", "", "", "", "", "", "", "", "", "", ""))
    writer.writeRow(Seq("9", "yandex", "НСИ", "Москва", "1",  "", "", "#yandex_link_codes_csv", "", "", "", "", "", "", "", "", "", ""))
    writer.writeRow(Seq("10", "yandex", "НСИ", "Москва", "1",  "", "", "#yandex_transfer_codes_csv", "", "", "", "", "", "", "", "", "", ""))
    writer.writeRow(Seq("11", "temp", "НСИ", "Москва", "1",  "", "", "#temp_entrance_station_codes_20171219", "", "", "", "", "", "", "", "", "", ""))
    writer.writeRow(Seq("12", "stats", "НСИ", "Москва", "1",  "", "", "#stats_time_m_csv", "", "", "", "", "", "", "", "", "", ""))
    val result = stream.toString("UTF-8")
    result
  }

  def edwexAll(): String = {
    import com.github.tototoshi.csv.DefaultCSVFormat
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = '\t'
      override val lineTerminator = "\n"
    }

    // https://github.com/tototoshi/scala-csv
    // https://www.programcreek.com/scala/java.io.ByteArrayOutputStream
    import com.github.tototoshi.csv.CSVWriter
    import java.io.ByteArrayOutputStream
    val stream = new ByteArrayOutputStream()
    val writer = CSVWriter.open(stream)
    writer.writeRow(Seq("1", "1", "METRO_GEO2", "LINE", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("1", "2", "METRO_GEO2", "LINECOLOR", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("1", "3", "METRO_GEO2", "NAME", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("1", "4", "METRO_GEO2", "LATITUDE", "NUMERIC", "n/c"))
    writer.writeRow(Seq("1", "5", "METRO_GEO2", "LONGITUDE", "NUMERIC", "n/c"))
    writer.writeRow(Seq("1", "6", "METRO_GEO2", "ORDER", "NUMERIC", "n/c"))

    writer.writeRow(Seq("2", "1", "MAXIMA_V20170615", "LINE_NUMBER", "NUMERIC", "n/c"))
    writer.writeRow(Seq("2", "2", "MAXIMA_V20170615", "STATION_NUMBER_ABS", "NUMERIC", "n/c"))
    writer.writeRow(Seq("2", "3", "MAXIMA_V20170615", "STATION_NAME_LONG_RU", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("2", "4", "MAXIMA_V20170615", "STATION_NAME_LONG_EN", "VARCHAR65535", "n/c"))

    writer.writeRow(Seq("3", "1", "DATA_LINE_CODES_V20171224", "LINE", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("3", "2", "DATA_LINE_CODES_V20171224", "ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("3", "3", "DATA_LINE_CODES_V20171224", "GLOBAL_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("3", "4", "DATA_LINE_CODES_V20171224", "STATUS", "VARCHAR65535", "n/c"))

    writer.writeRow(Seq("4", "1", "DATA_STATION_CODES_V20171224", "STATION", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("4", "2", "DATA_STATION_CODES_V20171224", "LINE", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("4", "3", "DATA_STATION_CODES_V20171224", "ADMAREA", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("4", "4", "DATA_STATION_CODES_V20171224", "GLOBAL_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("4", "5", "DATA_STATION_CODES_V20171224", "DISTRICT", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("4", "6", "DATA_STATION_CODES_V20171224", "STATUS", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("4", "7", "DATA_STATION_CODES_V20171224", "ID", "NUMERIC", "n/c"))

    writer.writeRow(Seq("5", "1", "DATA_ENTRANCE_STATION_CODES_V20171219", "LOCAL_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("5", "2", "DATA_ENTRANCE_STATION_CODES_V20171219", "NAME", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("5", "3", "DATA_ENTRANCE_STATION_CODES_V20171219", "LAT_WGS_84", "NUMERIC", "n/c"))
    writer.writeRow(Seq("5", "4", "DATA_ENTRANCE_STATION_CODES_V20171219", "LONG_WGS_84", "NUMERIC", "n/c"))
    writer.writeRow(Seq("5", "5", "DATA_ENTRANCE_STATION_CODES_V20171219", "STATION", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("5", "6", "DATA_ENTRANCE_STATION_CODES_V20171219", "LINE", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("5", "7", "DATA_ENTRANCE_STATION_CODES_V20171219", "EVEN_DAY_HOURS", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("5", "8", "DATA_ENTRANCE_STATION_CODES_V20171219", "ODD_DAY_HOURS", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("5", "9", "DATA_ENTRANCE_STATION_CODES_V20171219", "N_BPA_UNIVERSAL", "NUMERIC", "n/c"))
    writer.writeRow(Seq("5", "10", "DATA_ENTRANCE_STATION_CODES_V20171219", "N_BPA_1_2_ONLY", "NUMERIC", "n/c"))
    writer.writeRow(Seq("5", "11", "DATA_ENTRANCE_STATION_CODES_V20171219", "N_BPA_TOTAL", "NUMERIC", "n/c"))
    writer.writeRow(Seq("5", "12", "DATA_ENTRANCE_STATION_CODES_V20171219", "REPAIR_INFO", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("5", "13", "DATA_ENTRANCE_STATION_CODES_V20171219", "GLOBAL_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("5", "14", "DATA_ENTRANCE_STATION_CODES_V20171219", "GEO_DATA", "VARCHAR65535", "n/c"))

    writer.writeRow(Seq("6", "1", "DATA_PARKING_CODES_V20171220", "ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("6", "2", "DATA_PARKING_CODES_V20171220", "PARKING_NAME", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("6", "3", "DATA_PARKING_CODES_V20171220", "METRO_STATION", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("6", "4", "DATA_PARKING_CODES_V20171220", "GLOBAL_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("6", "5", "DATA_PARKING_CODES_V20171220", "METRO_LINE", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("6", "6", "DATA_PARKING_CODES_V20171220", "ADM_AREA", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("6", "7", "DATA_PARKING_CODES_V20171220", "DISTRICT", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("6", "8", "DATA_PARKING_CODES_V20171220", "LOCATION", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("6", "9", "DATA_PARKING_CODES_V20171220", "SCHEDULE", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("6", "10", "DATA_PARKING_CODES_V20171220", "ORG_PHONE", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("6", "11", "DATA_PARKING_CODES_V20171220", "CAR_CAPACITY", "NUMERIC", "n/c"))
    writer.writeRow(Seq("6", "12", "DATA_PARKING_CODES_V20171220", "LAT_WGS_84", "NUMERIC", "n/c"))
    writer.writeRow(Seq("6", "13", "DATA_PARKING_CODES_V20171220", "LONG_WGS_84", "NUMERIC", "n/c"))
    writer.writeRow(Seq("6", "14", "DATA_PARKING_CODES_V20171220", "GEO_DATA", "VARCHAR65535", "n/c"))

    writer.writeRow(Seq("7", "1", "YANDEX_LABEL_CODES", "LABEL_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("7", "2", "YANDEX_LABEL_CODES", "STATION_ID", "NUMERIC", "n/c"))

    writer.writeRow(Seq("8", "1", "YANDEX_LINE_CODES", "COLOR", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("8", "2", "YANDEX_LINE_CODES", "LINE_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("8", "3", "YANDEX_LINE_CODES", "NAME", "VARCHAR65535", "n/c"))

    writer.writeRow(Seq("9", "1", "YANDEX_STATION_CODES", "LABEL_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("9", "2", "YANDEX_STATION_CODES", "LINE_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("9", "3", "YANDEX_STATION_CODES", "LINK_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("9", "4", "YANDEX_STATION_CODES", "NAME", "VARCHAR65535", "n/c"))
    writer.writeRow(Seq("9", "5", "YANDEX_STATION_CODES", "STATION_ID", "NUMERIC", "n/c"))

    writer.writeRow(Seq("10", "1", "YANDEX_LINK_CODES", "FROM_STATION_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("10", "2", "YANDEX_LINK_CODES", "LINK_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("10", "3", "YANDEX_LINK_CODES", "TIME", "NUMERIC", "n/c"))
    writer.writeRow(Seq("10", "4", "YANDEX_LINK_CODES", "TO_STATION_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("10", "5", "YANDEX_LINK_CODES", "TRANSFER", "VARCHAR65536", "n/c"))
    writer.writeRow(Seq("10", "6", "YANDEX_LINK_CODES", "TRANSFER_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("10", "7", "YANDEX_LINK_CODES", "TYPE", "VARCHAR65536", "n/c"))

    writer.writeRow(Seq("11", "1", "YANDEX_TRANSFER_CODES", "STATION_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("11", "2", "YANDEX_TRANSFER_CODES", "TRANSFER_ID", "NUMERIC", "n/c"))

    writer.writeRow(Seq("-1", "1", "CASE_OWNERS", "STATION_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("-1", "2", "CASE_OWNERS", "TRANSFER_ID", "NUMERIC", "n/c"))

    writer.writeRow(Seq("-2", "1", "CASE_RECORDS", "STATION_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("-2", "2", "CASE_RECORDS", "TRANSFER_ID", "NUMERIC", "n/c"))

    writer.writeRow(Seq("-3", "1", "CASE_GRAPHS", "STATION_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("-3", "2", "CASE_GRAPHS", "TRANSFER_ID", "NUMERIC", "n/c"))

    writer.writeRow(Seq("-4", "1", "CASE_STATS", "STATION_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("-4", "2", "CASE_STATS", "TRANSFER_ID", "NUMERIC", "n/c"))

    writer.writeRow(Seq("-5", "1", "CASE_ROUTES", "STATION_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("-5", "2", "CASE_ROUTES", "TRANSFER_ID", "NUMERIC", "n/c"))

    writer.writeRow(Seq("-6", "1", "CASE_PROFILES", "STATION_ID", "NUMERIC", "n/c"))
    writer.writeRow(Seq("-6", "2", "CASE_PROFILES", "TRANSFER_ID", "NUMERIC", "n/c"))

    val result = stream.toString("UTF-8")
    result
  }

  def taskAll(): String = {
    import com.github.tototoshi.csv.DefaultCSVFormat
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = '\t'
      override val lineTerminator = "\n"
    }

    // https://github.com/tototoshi/scala-csv
    // https://www.programcreek.com/scala/java.io.ByteArrayOutputStream
    import com.github.tototoshi.csv.CSVWriter
    import java.io.ByteArrayOutputStream
    val stream = new ByteArrayOutputStream()
    val writer = CSVWriter.open(stream)

    writer.writeRow(Seq("(1) Номер ФТЗ", "(2) Наименование региона",
      "(3) Параметр (группа показателей, связанных первичным ключом)",
      "(4) Сложность (1 - существующий аттрибут, 2 - простой SQL, 3 - сложный SQL, 4 - machine learning)",
      "(5) Аттрибут таблицы источника",
      "(6) Система источник", "(7) Код источника", "(8) Назнание таблицы в источнике", "(9) Название таблицы в КХД",
      "(10) Логика", "(11) Комментарий"))
    // 1 to 57
    writer.writeRow(Seq("1", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("2", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("3", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("4", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("5", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("6", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("7", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("8", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("9", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("10", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("11", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("12", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("13", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("14", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("15", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("16", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("17", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("18", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("19", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("20", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("21", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("22", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("23", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("24", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("25", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("26", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("27", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("28", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("29", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("30", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("31", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("32", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("33", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("34", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("35", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("36", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("37", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("38", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("39", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("40", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("41", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("42", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("43", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("44", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("45", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("46", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("47", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("48", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("49", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("50", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("51", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("52", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("53", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("54", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("55", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("56", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    writer.writeRow(Seq("57", "", "FIRST", "4", "unknown", "public", "000001", "table", "dev_table", "_", "-"))
    val result = stream.toString("UTF-8")
    result
  }

  def main(args: Array[String]): Unit = {
    writeToFile("/home/user/CODE/access.csv", accessAll())
    writeToFile("/home/user/CODE/edwex.csv", edwexAll())
    writeToFile("/home/user/CODE/task.csv", taskAll())
  }
}