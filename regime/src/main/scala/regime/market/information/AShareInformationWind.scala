package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Information
import regime.market.Common._

object AShareInformationWind extends Information {
  lazy val query = RegimeSqlHelper.fromResource("sql/market/information/AShareInformationWind.sql")
  lazy val readFrom    = connMarketTable("ASHAREDESCRIPTION")
  lazy val saveTo      = connBizTable("ashare_information_wind")
  lazy val readFromCol = connMarketTableColumn("ASHAREDESCRIPTION", timeColumnMarket)
  lazy val saveToCol   = connBizTableColumn("ashare_information_wind", timeColumnBiz)
  lazy val primaryKey  = ("PK_ashare_information_wind", Seq("object_id"))
  lazy val index1      = ("IDX_ashare_information_wind_1", Seq(timeColumnBiz))
  lazy val index2      = ("IDX_ashare_information_wind_2", Seq("symbol"))

  lazy val compoundPK = Seq("object_id_ad", "object_id_aic", "object_id_ac")
  lazy val conversionFn = RegimeFn
    .dropNullRow(compoundPK)
    .andThen(RegimeFn.concatMultipleColumns("object_id", compoundPK, concatenateString))
    .andThen(RegimeFn.formatStringToDate("list_date", dateFormat))
    .andThen(RegimeFn.formatStringToDate("delist_date", dateFormat))
    .andThen(RegimeFn.formatStringToDate("entry_date", dateFormat))
    .andThen(RegimeFn.formatStringToDate("remove_date", dateFormat))

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
        throw new Exception("Invalid command")
    }
}
