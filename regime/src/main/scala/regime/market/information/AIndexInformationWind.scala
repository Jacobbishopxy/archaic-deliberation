package regime.market.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.market.Information
import regime.market.Common.{connMarket, connBizTable}

object AIndexInformationWind extends RegimeSpark with Information {
  lazy val query  = RegimeSqlHelper.fromResource("sql/market/information/AIndexInformationWind.sql")
  lazy val saveTo = "aindex_information_wind"
  lazy val primaryKeyName = "PK_aindex_information_wind"
  lazy val primaryColumn  = Seq("object_id")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    args.toList match {
      case Command.Initialize :: _ =>
        syncInitAll(connMarket, query, connBizTable(saveTo))
        createPrimaryKey(connBizTable(saveTo), primaryKeyName, primaryColumn)
      case Command.ExecuteOnce :: _ =>
        createPrimaryKey(connBizTable(saveTo), primaryKeyName, primaryColumn)
      case Command.SyncAll :: _ =>
        syncReplaceAll(connMarket, query, connBizTable(saveTo))
      case _ =>
        throw new Exception("Invalid Command")
    }
  }
}
