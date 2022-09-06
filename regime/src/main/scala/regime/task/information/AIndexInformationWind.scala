package regime.task.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.task.{Command, Information, RegimeTask}
import regime.task.Common.{connMarket, connBiz}

object AIndexInformationWind extends RegimeTask with Information {
  val appName: String = "AIndexInformationWind"

  val query = """
  AINDEXMEMBERSWIND
  AINDEXDESCRIPTION
  """

  val saveTo         = "aindex_information_wind"
  val primaryKeyName = "PK_aindex_information_wind"
  val primaryColumn  = Seq("object_id")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    //
  }
}
