package regime.helper

// TODO:
trait RegimeSparkSync {

  val query: String

  val fromTable: String

  val toTable: String

  val fromTableTimeColumn: String

  val toTableTimeColumn: String

  lazy val queryFromDate = (date: String) =>
    RegimeSqlHelper.generateQueryFromDate(query, fromTableTimeColumn, date)

  lazy val queryDateRange = (fromDate: String, toDate: String) =>
    RegimeSqlHelper.generateQueryDateRange(query, fromTableTimeColumn, (fromDate, toDate))

  lazy val queryAtDate = (date: String) =>
    RegimeSqlHelper.generateQueryAtDate(query, fromTableTimeColumn, date)

}
