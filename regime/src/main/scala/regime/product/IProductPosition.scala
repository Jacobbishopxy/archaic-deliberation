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

  lazy val readFrom          = "bside_ev_rpt_tradesummary"
  lazy val saveTo            = "iproduct_position"
  lazy val readUpdateCol     = "tradeDate"
  lazy val saveUpdateCol     = "trade_date"
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
    .formatLongToDatetime(saveUpdateCol, datetimeFormat)
    .andThen(RegimeFn.concatMultipleColumns(newPrimaryColName, newPKCols, concatenateString))

  // TODO：
  lazy val connProductExt = connProduct.optionsAppend(
    Map(
      // "partitionColumn" -> "trade_date",
      // "lowerBound"      -> "1000000",
      // "upperBound"      -> "10000000",
      // "numPartitions"   -> "4",
      "queryTimeout" -> "300",
      "fetchsize"    -> "1000"
    )
  )

  lazy val connBizExt = connBiz.optionsAppend(
    Map(
      "queryTimeout" -> "300",
      "batchsize"    -> "1000"
    )
  )

  def process(args: String*)(implicit spark: SparkSession): Unit = args.toList match {
    case Command.Initialize :: _ =>
      syncInitAll(
        connProductExt,
        query,
        connBizTable(saveTo),
        conversionFn
      )
      createPrimaryKeyAndIndex(
        connBizTable(saveTo),
        (primaryKeyName, primaryColumn),
        Seq(index1, index2)
      )
    case Command.ExecuteOnce :: _ =>
      createPrimaryKeyAndIndex(
        connBizTable(saveTo),
        (primaryKeyName, primaryColumn),
        Seq(index1, index2)
      )
    case Command.SyncFromLastUpdate :: _ =>
    // TODO:
    // trade_date has been converted to timestamp,
    // comparison between Long (original tradeDate type) and timestamp is incorrect

    // syncInsertFromLastUpdate(
    //   connProductTableColumn(readFrom, readUpdateCol),
    //   connBizTableColumn(saveTo, saveUpdateCol),
    //   queryFromDate,
    //   conversionFn
    // )
    case Command.OverrideFromLastUpdate :: _ =>
    // syncUpsertFromLastUpdate(
    //   connProductTableColumn(readFrom, readUpdateCol),
    //   connBizTableColumn(saveTo, saveUpdateCol),
    //   primaryColumn,
    //   queryFromDate,
    //   conversionFn
    // )
    case Command.TimeFromTillNowUpsert :: timeFrom :: _ =>
      syncUpsert(
        connProductExt,
        queryFromDate(timeFrom),
        connBizTable(saveTo),
        primaryColumn,
        conversionFn
      )
    case Command.TimeRangeUpsert :: timeFrom :: timeTo :: _ =>
      syncUpsert(
        connProductExt,
        queryDateRange(timeFrom, timeTo),
        connBizTable(saveTo),
        primaryColumn,
        conversionFn
      )
    case c @ _ =>
      log.error(c)
      throw new Exception("Invalid command")
  }
}
