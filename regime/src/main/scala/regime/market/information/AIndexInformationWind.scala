package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Information
import regime.market.Common._

object AIndexInformationWind extends Information {
  lazy val query = RegimeSqlHelper.fromResource("sql/market/information/AIndexInformationWind.sql")
  lazy val readFrom    = connMarketTable("AINDEXMEMBERSWIND")
  lazy val saveTo      = connBizTable("aindex_information_wind")
  lazy val readFromCol = connMarketTableColumn("AINDEXMEMBERSWIND", Token.timeColumnMarket)
  lazy val saveToCol   = connBizTableColumn("aindex_information_wind", Token.timeColumnBiz)
  lazy val primaryKey  = ("PK_aindex_information_wind", Seq(Token.objectId))
  lazy val index1      = ("IDX_aindex_information_wind_1", Seq(Token.timeColumnBiz))
  lazy val index2      = ("IDX_aindex_information_wind_2", Seq(Token.indexSymbol, Token.symbol))

  lazy val conversionFn = RegimeFn
    .formatStringToDate("in_date", dateFormat)
    .andThen(RegimeFn.formatStringToDate("out_date", dateFormat))
    .andThen(RegimeFn.formatStringToDate("expire_date", dateFormat))

  def process(args: String*)(implicit spark: SparkSession): Unit =
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
