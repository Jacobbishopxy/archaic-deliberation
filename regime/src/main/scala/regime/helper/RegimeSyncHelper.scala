package regime.helper

import regime.Conn
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

import regime.ConnTableColumn
import regime.DriverType

/** Get the latest update time from the target table, and query the rest of data from the resource
  * table.
  */
object RegimeSyncHelper {

  // ===============================================================================================
  // private helper functions
  // ===============================================================================================

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

  private def orderDir(isAsc: Boolean): String =
    if (isAsc) "ASC" else "DESC"

  private def generatePaginationStatement(
      conn: Conn,
      sql: String,
      pagination: Pagination
  ): String = sql + (
    conn.driverType match {
      case DriverType.MsSql =>
        s"""
        ORDER BY ${pagination.orderBy} ${orderDir(pagination.isAsc)}
        OFFSET ${pagination.offset} ROWS FETCH NEXT ${pagination.limit} ROWS ONLY
        """
      case DriverType.Postgres | DriverType.MySql =>
        s"""
        ORDER BY ${pagination.orderBy} ${orderDir(pagination.isAsc)} 
        LIMIT ${pagination.limit} OFFSET ${pagination.offset}
        """
      case DriverType.Other =>
        throw new Exception("Unsupported DriverType")
    }
  )

  // ===============================================================================================
  // general functions
  // 1. insertFromLastUpdateTime
  // 1. upsertFromLastUpdateTime
  // 1. batchInsert
  // 1. batchUpsert
  // ===============================================================================================

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

  /** Batch insert
    *
    * @param sourceConn
    * @param targetConn
    * @param sql
    * @param batchOption
    * @param conversionFn
    * @param spark
    */
  def batchInsert(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      sql: String,
      batchOption: BatchOption,
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = {
    // helpers
    val sourceHelper = RegimeJdbcHelper(sourceConn.conn)
    val targetHelper = RegimeJdbcHelper(targetConn.conn)

    // batching
    batchOption.genIterPagination().foreach { pg =>
      val stmt = generatePaginationStatement(sourceConn.conn, sql, pg)
      val df   = conversionFn(sourceHelper.readTable(stmt))

      targetHelper.saveTable(df, targetConn.table, SaveMode.Append)
    }
  }

  def batchInsert(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      sql: String,
      batchOption: BatchOption
  )(implicit spark: SparkSession): Unit =
    batchInsert(sourceConn, targetConn, sql, batchOption, df => df)

  /** Batch upsert
    *
    * @param sourceConn
    * @param targetConn
    * @param onConflictColumns
    * @param sql
    * @param batchOption
    * @param conversionFn
    * @param spark
    */
  def batchUpsert(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      onConflictColumns: Seq[String],
      sql: String,
      batchOption: BatchOption,
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = {
    // helpers
    val sourceHelper = RegimeJdbcHelper(sourceConn.conn)
    val targetHelper = RegimeJdbcHelper(targetConn.conn)

    // batching
    batchOption.genIterPagination().foreach { pg =>
      val stmt = generatePaginationStatement(sourceConn.conn, sql, pg)
      val df   = conversionFn(sourceHelper.readTable(stmt))

      targetHelper.upsertTable(
        df,
        targetConn.table,
        None,
        false,
        onConflictColumns,
        RegimeJdbcHelper.UpsertAction.DoUpdate
      )
    }
  }

  def batchUpsert(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      onConflictColumns: Seq[String],
      sql: String,
      batchOption: BatchOption
  )(implicit spark: SparkSession): Unit =
    batchUpsert(sourceConn, targetConn, onConflictColumns, sql, batchOption, df => df)

}

case class Pagination(
    orderBy: String,
    isAsc: Boolean,
    limit: Int,
    offset: Int
)

case class BatchOption(
    orderBy: String,
    isAsc: Boolean,
    lowerBound: Int, // greater than 0
    upperBound: Int, // greater than lowerBound
    callingTimes: Int
) {

  private def genPagination(i: Int): Pagination = {
    val size = upperBound - lowerBound
    Pagination(orderBy, isAsc, size * i, size * (i + 1))
  }

  def genIterPagination(): Iterator[Pagination] = {
    for {
      i <- (0 to callingTimes).iterator
    } yield genPagination(i)
  }
}

object BatchOption {
  def create(
      orderBy: String,
      isAsc: Boolean,
      lowerBound: Int,
      upperBound: Int,
      callingTimes: Int
  ): Option[BatchOption] = {
    if (lowerBound < 0 || upperBound < 0 || callingTimes < 0 || (lowerBound >= upperBound)) {
      None
    } else {
      Some(BatchOption(orderBy, isAsc, lowerBound, upperBound, callingTimes))
    }
  }
}
