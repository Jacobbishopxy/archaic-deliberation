package regime.task.information

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper.RegimeJdbcHelper
import regime.task.{Command, Information, RegimeTask}
import regime.task.Common.{connMarket, connBiz}

object AIndexInformationCitics extends RegimeTask with Information {
  val appName: String = "AIndexInformationCitics"

  val query = """
  AINDEXMEMBERSCITICS
  AINDEXDESCRIPTION
  """

  val saveTo         = "aindex_information_scitics"
  val primaryKeyName = "PK_aindex_information_scitics"
  val primaryColumn  = Seq("object_id")

  def process(args: String*)(implicit spark: SparkSession): Unit = {
    //
  }

}
