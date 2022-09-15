package regime.product

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

import regime.helper._
import regime.product.Common._

object IProductPosition extends RegimeSpark with Product {
  lazy val query = RegimeSqlHelper.fromResource("sql/product/IProductPosition.sql")
  lazy val queryFromDate = (date: String) => query + s"""
  WHERE tradeDate > '$date'
  """
  lazy val queryDateRange = (fromDate: String, toDate: String) => query + s"""
  WHERE tradeDate > '$fromDate' AND tradeDate < '$toDate'
  """
  lazy val queryAtDate = (date: String) => query + s"""
  WHERE tradeDate = '$date'
  """

  lazy val readFrom          = connProductTable("bside_ev_rpt_tradesummary")
  lazy val saveTo            = connBizTable("iproduct_position")
  lazy val readFromCol       = connProductTableColumn("bside_ev_rpt_tradesummary", "tradeDate")
  lazy val saveToCol         = connBizTableColumn("iproduct_position", "trade_date")
  lazy val primaryKeyName    = "PK_iproduct_position"
  lazy val newPrimaryColName = "object_id"
  lazy val newPKCols = Seq(
    "trade_date",
    "product_num",
    "product_account_type",
    "exch_id",
    "stock_id",
    "bs_flag",
    "hedge_flag"
  )
  lazy val primaryColumn = Seq("object_id")
  lazy val index1 = (
    "IDX_iproduct_position_1",
    Seq("trade_date", "product_num", "parent_product_num", "exch_id", "stock_id")
  )
  lazy val index2 = (
    "IDX_iproduct_position_2",
    Seq("trade_date", "exch_id", "stock_id")
  )

  lazy val conversionFn = RegimeFn
    .formatLongToDatetime("trade_date", datetimeFormat)
    .andThen(RegimeFn.concatMultipleColumns(newPrimaryColName, newPKCols, concatenateString))

  def process(args: String*)(implicit spark: SparkSession): Unit = args.toList match {
    case Command.Initialize :: _ =>
      // TODO
      syncInitAll(readFrom, saveTo, query, None)
      createPrimaryKeyAndIndex(
        saveTo,
        (primaryKeyName, primaryColumn),
        Seq(index1, index2)
      )
    case Command.ExecuteOnce :: _ =>
      createPrimaryKeyAndIndex(
        saveTo,
        (primaryKeyName, primaryColumn),
        Seq(index1, index2)
      )
    case Command.SyncFromLastUpdate :: _ =>
    // TODO:
    // trade_date has been converted to timestamp,
    // comparison between Long (original tradeDate type) and timestamp is incorrect
    case Command.OverrideFromLastUpdate :: _ =>
    // TODO
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
    case c @ _ =>
      log.error(c)
      throw new Exception("Invalid command")
  }
}
