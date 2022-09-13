package regime.helper

import regime.Conn
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

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

  private lazy val sourceViewName = "SOURCE_DF"
  private lazy val targetViewName = "TARGET_DF"

  private def lastUpdateTimeCurrying(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn
  )(
      fn: (RegimeJdbcHelper, RegimeJdbcHelper, String) => Unit
  )(implicit spark: SparkSession): Unit = {

    // helpers
    val sourceHelper = RegimeJdbcHelper(sourceConn.conn)
    val targetHelper = RegimeJdbcHelper(targetConn.conn)

    // get latest date from both tables
    val sourceDf = sourceHelper.readTable(getMaxDate(sourceConn.table, sourceConn.column))
    val targetDf = targetHelper.readTable(getMaxDate(targetConn.table, targetConn.column))

    // create temp views
    sourceDf.createOrReplaceTempView(sourceViewName)
    targetDf.createOrReplaceTempView(targetViewName)

    // first value from each view
    val sourceFirstValue = getFirstValue(sourceViewName, sourceConn.column)
    val targetFirstValue = getFirstValue(targetViewName, targetConn.column)

    val resRow = spark
      .sql(s"""SELECT if(($sourceFirstValue) > ($targetFirstValue),($targetFirstValue),NULL)""")
      .toDF()
      .first()

    if (resRow.isNullAt(0)) {
      None
    } else {
      val lastDate = resRow.get(0).toString()
      Some(fn(sourceHelper, targetHelper, lastDate))
    }
  }

  /** Insert from last update time.
    *
    * @param sourceConn
    * @param targetConn
    * @param querySqlCst
    * @param conversionFn
    * @param spark
    * @return
    */
  def insertFromLastUpdateTime(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      querySqlCst: String => String,
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = lastUpdateTimeCurrying(sourceConn, targetConn) {
    (
        sourceHelper,
        targetHelper,
        lastUpdateTime
    ) =>
      // DataFrame from the last update point
      val df = conversionFn(sourceHelper.readTable(querySqlCst(lastUpdateTime)))

      // Saving date into target table
      targetHelper.saveTable(df, targetConn.table, SaveMode.Append)
  }

  def insertFromLastUpdateTime(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      querySqlCst: String => String
  )(implicit spark: SparkSession): Unit =
    insertFromLastUpdateTime(sourceConn, targetConn, querySqlCst, df => df)

  /** Upsert from last update time.
    *
    * @param sourceConn
    * @param targetConn
    * @param onConflictColumns
    * @param querySqlCst
    * @param conversionFn
    * @param spark
    * @return
    */
  def upsertFromLastUpdateTime(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      onConflictColumns: Seq[String],
      querySqlCst: String => String,
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = lastUpdateTimeCurrying(sourceConn, targetConn) {
    (
        sourceHelper,
        targetHelper,
        lastUpdateTime
    ) =>
      // DataFrame from the last update point
      val df = conversionFn(sourceHelper.readTable(querySqlCst(lastUpdateTime)))

      // Saving date into target table
      targetHelper.upsertTable(
        df,
        targetConn.table,
        None,
        false,
        onConflictColumns,
        RegimeJdbcHelper.UpsertAction.DoUpdate
      )
  }

  def upsertFromLastUpdateTime(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      onConflictColumns: Seq[String],
      querySqlCst: String => String
  )(implicit spark: SparkSession): Unit =
    upsertFromLastUpdateTime(sourceConn, targetConn, onConflictColumns, querySqlCst, df => df)

}
