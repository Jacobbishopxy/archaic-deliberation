package regime.helper

import regime.Conn
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType}
import org.apache.spark.sql.SaveMode
import org.apache.spark.util.SizeEstimator
import regime.ConnTableColumn

/** Get the latest update time from the target table, and query the rest of data from the resource
  * table.
  */
object RegimeTimeHelper {
  private def getMaxDate(tableName: String, columnName: String) = s"""
    SELECT MAX($columnName) AS max_$columnName FROM $tableName
    """

  private def getFirstValue(tableName: String, columnName: String) = s"""
    SELECT first_value(max_$columnName) FROM $tableName
    """

  def insertFromLastUpdateTime(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      querySqlCst: Any => String
  )(implicit spark: SparkSession): Option[Long] = {
    val sourceHelper = RegimeJdbcHelper(sourceConn.conn)
    val targetHelper = RegimeJdbcHelper(targetConn.conn)

    val sourceDf = sourceHelper.readTable(getMaxDate(sourceConn.table, sourceConn.column))
    val targetDf = targetHelper.readTable(getMaxDate(targetConn.table, targetConn.column))

    sourceDf.createOrReplaceTempView("SOURCE_DF")
    targetDf.createOrReplaceTempView("TARGET_DF")

    val sourceFirstValue = getFirstValue(sourceConn.table, sourceConn.column)
    val targetFirstValue = getFirstValue(targetConn.table, targetConn.column)

    val resRow = spark
      .sql(s"""SELECT if(($sourceFirstValue) > ($targetFirstValue),($targetFirstValue),NULL)""")
      .toDF()
      .first()

    if (resRow.isNullAt(0)) {
      return None
    }

    val updateFrom = resRow.get(0)

    val df = sourceHelper.readTable(querySqlCst(updateFrom))

    val size = SizeEstimator.estimate(df)

    sourceHelper.saveTable(df, targetConn.table, SaveMode.Append)

    Some(size)
  }
}
