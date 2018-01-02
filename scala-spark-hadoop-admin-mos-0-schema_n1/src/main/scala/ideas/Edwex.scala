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
    // https://github.com/tototoshi/scala-csv
    // https://www.programcreek.com/scala/java.io.ByteArrayOutputStream
    import com.github.tototoshi.csv.CSVWriter
    import java.io.ByteArrayOutputStream
    val stream = new ByteArrayOutputStream()
    val writer = CSVWriter.open(stream)
    writer.writeRow(Seq("1", "1", "METRO_GEO2", "LINE", "VARCHAR65535"))
    writer.writeRow(Seq("1", "2", "METRO_GEO2", "LINECOLOR", "VARCHAR65535"))
    writer.writeRow(Seq("1", "3", "METRO_GEO2", "NAME", "VARCHAR65535"))
    writer.writeRow(Seq("1", "4", "METRO_GEO2", "LATITUDE", "NUMERIC"))
    writer.writeRow(Seq("1", "5", "METRO_GEO2", "LONGITUDE", "NUMERIC"))
    writer.writeRow(Seq("1", "6", "METRO_GEO2", "ORDER", "NUMERIC"))

    writer.writeRow(Seq("2", "1", "MAXIMA_20170615", "LINE_NUMBER", "NUMERIC"))
    writer.writeRow(Seq("2", "2", "MAXIMA_20170615", "STATION_NUMBER_ABS", "NUMERIC"))
    writer.writeRow(Seq("2", "3", "MAXIMA_20170615", "STATION_NAME_LONG_RU", "VARCHAR65535"))
    writer.writeRow(Seq("2", "4", "MAXIMA_20170615", "STATION_NAME_LONG_EN", "VARCHAR65535"))

    writer.writeRow(Seq("3", "1", "DATA_LINE_CODES_20171224", "LINE", "VARCHAR65535"))
    writer.writeRow(Seq("3", "2", "DATA_LINE_CODES_20171224", "ID", "NUMERIC"))
    writer.writeRow(Seq("3", "3", "DATA_LINE_CODES_20171224", "GLOBAL_ID", "NUMERIC"))
    writer.writeRow(Seq("3", "4", "DATA_LINE_CODES_20171224", "STATUS", "VARCHAR65535"))

    writer.writeRow(Seq("4", "1", "DATA_STATION_CODES_20171224", "STATION", "VARCHAR65535"))
    writer.writeRow(Seq("4", "2", "DATA_STATION_CODES_20171224", "LINE", "VARCHAR65535"))
    writer.writeRow(Seq("4", "3", "DATA_STATION_CODES_20171224", "ADMAREA", "VARCHAR65535"))
    writer.writeRow(Seq("4", "4", "DATA_STATION_CODES_20171224", "GLOBAL_ID", "NUMERIC"))
    writer.writeRow(Seq("4", "5", "DATA_STATION_CODES_20171224", "DISTRICT", "VARCHAR65535"))
    writer.writeRow(Seq("4", "6", "DATA_STATION_CODES_20171224", "STATUS", "VARCHAR65535"))
    writer.writeRow(Seq("4", "7", "DATA_STATION_CODES_20171224", "ID", "NUMERIC"))

    writer.writeRow(Seq("5", "1", "DATA_ENTRANCE_STATION_CODES_20171219", "lokal_nyj_identifikator", "VARCHAR65535"))
    writer.writeRow(Seq("5", "2", "DATA_ENTRANCE_STATION_CODES_20171219", "naimenovanie", "NUMERIC"))
    writer.writeRow(Seq("5", "3", "DATA_ENTRANCE_STATION_CODES_20171219", "dolgota_v_wgs_84", "NUMERIC"))
    writer.writeRow(Seq("5", "4", "DATA_ENTRANCE_STATION_CODES_20171219", "shirota_v_wgs_84", "VARCHAR65535"))
    writer.writeRow(Seq("5", "5", "DATA_ENTRANCE_STATION_CODES_20171219", "liniya", "VARCHAR65535"))
    writer.writeRow(Seq("5", "6", "DATA_ENTRANCE_STATION_CODES_20171219", "rezhim_raboty_po_chyotnym_dnyam", "NUMERIC"))
    writer.writeRow(Seq("5", "7", "DATA_ENTRANCE_STATION_CODES_20171219", "rezhim_raboty_po_nechyotnym_dnyam", "NUMERIC"))
    writer.writeRow(Seq("5", "8", "DATA_ENTRANCE_STATION_CODES_20171219", "kolichestvo_polnofunkcional_nyh_bpa_vse_tipy_biletov", "VARCHAR65535"))
    writer.writeRow(Seq("5", "9", "DATA_ENTRANCE_STATION_CODES_20171219", "kolichestvo_malofunkcional_nyh_bpa__bilety_na_1_i_2_poezdki", "NUMERIC"))
    writer.writeRow(Seq("5", "10", "DATA_ENTRANCE_STATION_CODES_20171219", "obshchee_kolichestvo_bpa", "NUMERIC"))
    writer.writeRow(Seq("5", "11", "DATA_ENTRANCE_STATION_CODES_20171219", "remont_ehskalatorov", "VARCHAR65535"))
    writer.writeRow(Seq("5", "12", "DATA_ENTRANCE_STATION_CODES_20171219", "global_id", "NUMERIC"))
    writer.writeRow(Seq("5", "13", "DATA_ENTRANCE_STATION_CODES_20171219", "geodannye", "VARCHAR65535"))

    writer.writeRow(Seq("3", "1", "LINE_CODES_CSV", "COLOR", "VARCHAR65535"))
    writer.writeRow(Seq("3", "2", "LINE_CODES_CSV", "LINE_ID", "NUMERIC"))
    writer.writeRow(Seq("3", "3", "LINE_CODES_CSV", "NAME", "VARCHAR65535"))
    writer.writeRow(Seq("4", "1", "STATION_CODES_CSV", "LABEL_ID", "NUMERIC"))
    writer.writeRow(Seq("4", "2", "STATION_CODES_CSV", "LINE_ID", "NUMERIC"))
    writer.writeRow(Seq("4", "3", "STATION_CODES_CSV", "LINK_ID", "NUMERIC"))
    writer.writeRow(Seq("4", "4", "STATION_CODES_CSV", "NAME", "VARCHAR65535"))
    writer.writeRow(Seq("4", "5", "STATION_CODES_CSV", "STATION_ID", "NUMERIC"))
    writer.writeRow(Seq("5", "1", "ENTRANCE_STATION_CODES_CSV", "ENTRANCE_ID", "NUMERIC"))
    writer.writeRow(Seq("5", "2", "ENTRANCE_STATION_CODES_CSV", "LINE_NAME", "NUMERIC"))
    writer.writeRow(Seq("5", "3", "ENTRANCE_STATION_CODES_CSV", "STATION_NAME", "NUMERIC"))
    val result = stream.toString("UTF-8")
    result
  }

  def main(args: Array[String]): Unit = {
    import com.github.tototoshi.csv.DefaultCSVFormat
    implicit object MyFormat extends DefaultCSVFormat {
      override val delimiter = '\t'
      override val lineTerminator = "\n"
    }
    writeToFile("/home/user/CODE/access.csv", accessAll())
    writeToFile("/home/user/CODE/edwex.csv", edwexAll())
  }
}
