package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Information
import regime.market.Common._

object AIndexInformation extends Information {
  lazy val query    = RegimeSqlHelper.fromResource("sql/market/information/AIndexInformation.sql")
  lazy val readFrom = connMarketTable("AINDEXMEMBERS")
  lazy val saveTo   = connBizTable("aindex_information")
  lazy val readFromCol = connMarketTableColumn("AINDEXMEMBERS", timeColumnMarket)
  lazy val saveToCol   = connBizTableColumn("aindex_information", timeColumnBiz)
  lazy val primaryKey  = ("PK_aindex_information", Seq("object_id"))
  lazy val index1      = ("IDX_aindex_information_1", Seq(timeColumnBiz))
  lazy val index2      = ("IDX_aindex_information_2", Seq("index_symbol", "symbol"))

  lazy val conversionFn = RegimeFn
    .formatStringToDate("in_date", dateFormat)
    .andThen(RegimeFn.formatStringToDate("out_date", dateFormat))
    .andThen(RegimeFn.formatStringToDate("expire_date", dateFormat))

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None, conversionFn)
        createPrimaryKeyAndIndex(
          saveTo,
          primaryKey,
          Seq(index1, index2)
        )
      case Command.ExecuteOnce :: _ =>
        createPrimaryKeyAndIndex(
          saveTo,
          primaryKey,
          Seq(index1, index2)
        )
      case Command.SyncAll :: _ =>
        syncReplaceAllIfUpdated(readFromCol, saveToCol, query, None, conversionFn)
      case _ =>
        throw new Exception("Invalid Command")
    }
  }
}
