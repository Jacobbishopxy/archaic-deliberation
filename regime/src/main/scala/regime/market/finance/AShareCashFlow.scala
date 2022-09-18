package regime.market.finance

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Finance
import regime.market.Common.{connMarketTable, connBizTable}

object AShareCashFlow extends Finance {
  lazy val query = RegimeSqlHelper.fromResource("sql/market/finance/AShareCashFlow.sql")

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  lazy val readFrom       = connMarketTable("ASHARECASHFLOW")
  lazy val saveTo         = connBizTable("ashare_cashflow")
  lazy val primaryKeyName = "PK_ashare_cashflow"
  lazy val primaryColumn  = Seq("object_id")
  lazy val indexName1     = "IDX_ashare_cashflow_1"
  lazy val indexName2     = "IDX_ashare_cashflow_2"
  lazy val indexColumn1   = Seq("update_date")
  lazy val indexColumn2   = Seq("report_period", "symbol")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None)
        createPrimaryKeyAndIndex(
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq((indexName1, indexColumn1), (indexName2, indexColumn2))
        )
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          saveTo,
          (primaryKeyName, primaryColumn),
          Seq((indexName1, indexColumn1), (indexName2, indexColumn2))
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          readFrom,
          saveTo,
          queryFromDate(timeFrom),
          primaryColumn,
          None
        )
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        syncUpsert(
          readFrom,
          saveTo,
          queryDateRange(timeFrom, timeTo),
          primaryColumn,
          None
        )
      case _ => throw new Exception("Invalid command")
    }
  }
}
