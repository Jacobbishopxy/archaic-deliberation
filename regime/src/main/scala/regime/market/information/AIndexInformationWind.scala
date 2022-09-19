package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Information
import regime.market.Common._

object AIndexInformationWind extends Information {
  lazy val query = RegimeSqlHelper.fromResource("sql/market/information/AIndexInformationWind.sql")
  lazy val readFrom       = connMarketTable("AINDEXMEMBERSWIND")
  lazy val saveTo         = connBizTable("aindex_information_wind")
  lazy val readFromCol    = connMarketTableColumn("AINDEXMEMBERSWIND", timeColumnMarket)
  lazy val saveToCol      = connBizTableColumn("aindex_information_wind", timeColumnBiz)
  lazy val primaryKeyName = "PK_aindex_information_wind"
  lazy val primaryColumn  = Seq("object_id")

  def process(args: String*)(implicit spark: SparkSession): Unit =
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None)
        createPrimaryKey(saveTo, primaryKeyName, primaryColumn)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKey(saveTo, primaryKeyName, primaryColumn)
      case Command.SyncAll :: _ =>
        syncReplaceAllIfUpdated(readFromCol, saveToCol, query, None)
      case _ =>
        throw new Exception("Invalid Command")
    }
}
