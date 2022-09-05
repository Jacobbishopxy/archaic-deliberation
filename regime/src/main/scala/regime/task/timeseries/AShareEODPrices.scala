package regime.task.timeseries

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.task.Common.{connMarket, connBiz}
import regime.helper._
import regime.task.{Command, TimeSeries}

object AShareEODPrices extends RegimeSpark with TimeSeries {
  val appName: String = "AShareEODPrices"

  val query = """
  SELECT
    S_INFO_WINDCODE AS symbol,
    TRADE_DT AS trade_date,
    CRNCY_CODE AS currency,
    S_DQ_PRECLOSE AS pre_close,
    S_DQ_OPEN AS open,
    S_DQ_HIGH AS high,
    S_DQ_LOW AS low,
    S_DQ_CLOSE AS close,
    S_DQ_CHANGE AS change,
    S_DQ_PCTCHANGE AS percent_change,
    S_DQ_VOLUME AS volume,
    S_DQ_AMOUNT AS amount,
    S_DQ_ADJPRECLOSE AS adj_pre_close,
    S_DQ_ADJOPEN AS adj_open,
    S_DQ_ADJHIGH AS adj_high,
    S_DQ_ADJLOW AS adj_low,
    S_DQ_ADJCLOSE AS adj_close,
    S_DQ_ADJFACTOR AS adj_factor,
    S_DQ_AVGPRICE AS average_price,
    S_DQ_TRADESTATUS AS trade_status,
    S_DQ_TRADESTATUSCODE AS trade_status_code,
    S_DQ_LIMIT AS limit_price,
    S_DQ_STOPPING AS stopping_price,
    OPDATE AS update_date
  FROM
    ASHAREEODPRICES
  """

  lazy val queryFromDate = (date: String) => query + s"""
  WHERE OPDATE > '$date'
  """

  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE OPDATE > '$fromDate' AND OPDATE < '$toDate'
  """

  val saveTo         = "ashare_eod_prices"
  val primaryKeyName = "PK_ashare_eod_prices"
  val primaryColumn  = Seq("object_id")
  val indexName1     = "IDX_ashare_eod_prices_1"
  val indexName2     = "IDX_ashare_eod_prices_2"
  val indexColumn1   = Seq("update_date")
  val indexColumn2   = Seq("trade_date", "symbol")

  def process(spark: SparkSession, args: String*): Unit = {
    args.toList match {
      case Command.SyncAll :: _                    => syncAll(spark)
      case Command.ExecuteOnce :: _                => createPrimaryKeyAndIndex()
      case Command.TimeFromUpsert :: timeFrom :: _ => upsertFromDate(spark, timeFrom)
      case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
        upsertDateRange(spark, timeFrom, timeTo)
      case _ => throw new Exception("Invalid command")
    }
  }

  private def syncAll(spark: SparkSession): Unit = {
    val df = RegimeJdbcHelper(connMarket).readTable(spark, query)

    RegimeJdbcHelper(connBiz).saveTable(df, saveTo, SaveMode.Overwrite)
  }

  private def createPrimaryKeyAndIndex(): Unit = {
    val helper = RegimeJdbcHelper(connBiz)

    helper.createPrimaryKey(saveTo, primaryKeyName, primaryColumn)

    helper.createIndex(saveTo, indexName1, indexColumn1)
    helper.createIndex(saveTo, indexName2, indexColumn2)
  }

  private def upsertFromDate(spark: SparkSession, fromDate: String): Unit = {
    val df = RegimeJdbcHelper(connMarket).readTable(spark, queryFromDate(fromDate))

    RegimeJdbcHelper(connBiz).upsertTable(df, saveTo, None, false, primaryColumn, "doUpdate")
  }

  private def upsertDateRange(spark: SparkSession, fromDate: String, toDate: String): Unit = {
    val df = RegimeJdbcHelper(connMarket).readTable(spark, queryDateRange(fromDate, toDate))

    RegimeJdbcHelper(connBiz).upsertTable(df, saveTo, None, false, primaryColumn, "doUpdate")
  }
}
