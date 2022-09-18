package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Information
import regime.market.Common.{connMarketTable, connBizTable}

object AIndexInformationCitics extends Information {
  lazy val query =
    RegimeSqlHelper.fromResource("sql/market/information/AIndexInformationCitics.sql")
  lazy val readFrom       = connMarketTable("AINDEXMEMBERSCITICS")
  lazy val saveTo         = connBizTable("aindex_information_citics")
  lazy val primaryKeyName = "PK_aindex_information_citics"
  lazy val primaryColumn  = Seq("object_id")

  def process(args: String*)(implicit spark: SparkSession): Unit =
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(readFrom, saveTo, query, None)
        createPrimaryKey(saveTo, primaryKeyName, primaryColumn)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKey(saveTo, primaryKeyName, primaryColumn)
      case Command.SyncAll :: _ =>
        syncReplaceAll(readFrom, saveTo, query, None)
      case _ =>
        throw new Exception("Invalid Command")
    }
}
