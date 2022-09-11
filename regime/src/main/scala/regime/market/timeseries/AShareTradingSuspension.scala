package regime.market.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.TimeSeries
import regime.market.Common._

object AShareTradingSuspension extends RegimeSpark with TimeSeries {
  lazy val query = """
  SELECT
    OBJECT_ID as object_id,
    S_INFO_WINDCODE AS symbol,
    S_DQ_SUSPENDDATE AS suspend_date,
    S_DQ_SUSPENDTYPE AS suspend_type,
    S_DQ_RESUMEPDATE AS resume_date,
    S_DQ_CHANGEREASON AS reason,
    S_DQ_TIME AS suspend_time,
    OPDATE AS update_date
  FROM
    ASHARETRADINGSUSPENSION
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val readFrom       = "ASHARETRADINGSUSPENSION"
  lazy val saveTo         = "ashare_trading_suspension"
  lazy val readUpdateCol  = "OPDATE"
  lazy val saveUpdateCol  = "update_date"
  lazy val primaryKeyName = "PK_ashare_trading_suspension"
  lazy val primaryColumn  = Seq("object_id")
  lazy val index          = ("IDX_ashare_trading_suspension", Seq("update_date"))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(connMarket, query, connBizTable(saveTo))
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          connBizTable(saveTo),
          (primaryKeyName, primaryColumn),
          Seq(index)
        )
      case Command.SyncFromLastUpdate :: _ =>
        syncInsertFromLastUpdate(
          connMarketTableColumn(readFrom, readUpdateCol),
          connBizTableColumn(saveTo, saveUpdateCol),
          queryFromDate
        )
      case Command.OverrideFromLastUpdate :: _ =>
        syncUpsertFromLastUpdate(
          connMarketTableColumn(readFrom, readUpdateCol),
          connBizTableColumn(saveTo, saveUpdateCol),
          primaryColumn,
          queryFromDate
        )
      case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
        syncUpsert(
          connMarket,
          queryFromDate(timeFrom),
          connBizTable(saveTo),
          primaryColumn
        )
      case c @ _ =>
        log.error(c)
        throw new Exception("Invalid command")
    }
  }
}
