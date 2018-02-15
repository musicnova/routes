package ideas

import java.sql.Date

object UtilWifi0Errors extends Serializable {
  case class RecError(id: Option[Int], status: String, casePart: String,
                      datePart: Date, regionPart: String,
                      typePart: String, tablePart: String, filePath: String,
                      volume: String, columns: String, since: String, till: String,
                      tags: String, description: Option[String], deleted: Long)
  /*
def readErrors(ssh: UtilWifi0Dicts.How, filePath: String, datePart: String, caseLabel: String): Stream[RecError] = {

  val content = SSH(ssh.host, HostConfig(PasswordLogin(ssh.user, ssh.password), port = ssh.port)) {
    client => {
      // READ a/11616581/7713755
      client.client.addHostKeyVerifier("66:80:54:96:a5:77:51:43:17:c2:ae:22:fe:4a:96:7c")
      client.exec(s"hdfs dfs -cat $filePath| gzip -d").right.map(_.stdOutAsString())
    }
  }.merge.split("\n")
  (if (true/*isHeader*/) content.tail else content)
    .map(_.split("\t"/*clmSep*/))
    .collect {
      case Array(status, volume, tags, since, till, sinceName, tillName, tableName, dateName, regionName,
      stageName, levelName, typeName, hashName, columns, description, filePath, regionPart, tablePart, typePart) =>
        ReadStatus(0, None, status, caseLabel,
          if (dateName != datePart) {
            new java.sql.Date(0)
          } else {
            new java.sql.Date(new java.text.SimpleDateFormat("yyyyMMdd").parse(datePart).getTime)
          },
          regionPart, typePart, tablePart, filePath, volume, columns, since, till, tags, None)
    }.toStream
    .filter(st => st.tags.contains("BDT-17;") && st.datePart != new java.sql.Date(0)
      && UtilBDT17DictDBLoader.readDict(filePath, datePart).filter(d => d.state == "ACTIVE")
      .map(d => d.key).contains(st.tablePart))

    val res: List[RecError] = List[RecError]()
    res.toStream
  }
    */
}
