package regime.helper

import regime.Conn
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import regime.{ConnTable, ConnTableColumn, DriverType}

/** Get the latest update time from the target table, and query the rest of data from the resource
  * table.
  */
object RegimeSyncHelper {

  // ===============================================================================================
  // private helper functions
  // ===============================================================================================

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

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

  private def generateCountFromStatement(column: String, table: String): String =
    s"""
    SELECT COUNT(${column}) FROM ${table}
    """

  private def getMaxDate(tableName: String, columnName: String) = s"""
    SELECT MAX($columnName) AS max_$columnName FROM $tableName
    """

  private def getFirstValue(tableName: String, columnName: String) = s"""
    SELECT first_value(max_$columnName) FROM $tableName
    """

  private lazy val sourceViewName = "SOURCE_DF"
  private lazy val targetViewName = "TARGET_DF"

  // TODO:
  // should add function to handle than datetime format are not the same
  private def lastUpdateTimeCurrying(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn
  )(
      fn: (RegimeJdbcHelper, RegimeJdbcHelper, String) => Unit
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting get last update time...")
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
      log.info(s"Last date: $lastDate")
      Some(fn(sourceHelper, targetHelper, lastDate))
    }
  }

  // ===============================================================================================
  // general functions
  // 1. generateBatchOption
  // 1. batchInsert
  // 1. batchUpsert
  // 1. insertFromLastUpdateTime
  // 1. upsertFromLastUpdateTime
  // ===============================================================================================

  /** Generate BatchOption by counting the maximum size of a table
    *
    * @param ctc
    * @param isAsc
    * @param fetchSize
    * @param spark
    * @return
    */
  def generateBatchOption(
      ctc: ConnTableColumn,
      isAsc: Boolean,
      fetchSize: Int
  )(implicit spark: SparkSession): Option[BatchOption] = {
    log.info("Starting generate BatchOption...")
    val helper = RegimeJdbcHelper(ctc.conn)
    val sql    = generateCountFromStatement(ctc.column, ctc.table)

    val rowsOfTable  = helper.readTable(sql).first().get(0).asInstanceOf[Long]
    val callingTimes = math.floor(rowsOfTable / fetchSize).toInt
    log.info(s"CallingTimes: $callingTimes")

    BatchOption.create(ctc.column, isAsc, fetchSize, callingTimes)
  }

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
      sourceConn: ConnTable,
      targetConn: ConnTable,
      sql: String,
      batchOption: BatchOption,
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting BatchInsert...")
    // helpers
    val sourceHelper = RegimeJdbcHelper(sourceConn.conn)
    val targetHelper = RegimeJdbcHelper(targetConn.conn)

    // batching
    batchOption.genIterPagination.zipWithIndex.foreach { case (pg, idx) =>
      log.info(s"Batching num: $idx")
      log.info(s"Batching pagination: $pg")
      val stmt = generatePaginationStatement(sourceConn.conn, sql, pg)
      val df   = conversionFn(sourceHelper.readTable(stmt))

      targetHelper.saveTable(df, targetConn.table, SaveMode.Append)
      log.info(s"Batching $idx saved complete")
    }
    log.info("BatchInsert complete!")
  }

  def batchInsert(
      sourceConn: ConnTable,
      targetConn: ConnTable,
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
      sourceConn: ConnTable,
      targetConn: ConnTable,
      onConflictColumns: Seq[String],
      sql: String,
      batchOption: BatchOption,
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting BatchUpsert...")
    // helpers
    val sourceHelper = RegimeJdbcHelper(sourceConn.conn)
    val targetHelper = RegimeJdbcHelper(targetConn.conn)

    // batching
    batchOption.genIterPagination.zipWithIndex.foreach { case (pg, idx) =>
      log.info(s"Batching num: $idx")
      log.info(s"Batching pagination: $pg")
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
      log.info(s"Batching $idx saved complete")
    }
    log.info("BatchUpsert complete!")
  }

  def batchUpsert(
      sourceConn: ConnTable,
      targetConn: ConnTable,
      onConflictColumns: Seq[String],
      sql: String,
      batchOption: BatchOption
  )(implicit spark: SparkSession): Unit =
    batchUpsert(sourceConn, targetConn, onConflictColumns, sql, batchOption, df => df)

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
      batchOption: Option[BatchOption],
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = lastUpdateTimeCurrying(sourceConn, targetConn) {
    (
        sourceHelper,
        targetHelper,
        lastUpdateTime
    ) =>
      log.info("Starting InsertFromLastUpdateTime...")
      batchOption match {
        case None =>
          // DataFrame from the last update point
          val df = conversionFn(sourceHelper.readTable(querySqlCst(lastUpdateTime)))
          // Saving date into target table
          targetHelper.saveTable(df, targetConn.table, SaveMode.Append)
        case Some(bo) =>
          log.info("Starting BatchInsert...")
          bo.genIterPagination.zipWithIndex.foreach { case (pg, idx) =>
            log.info(s"Batching num: $idx")
            log.info(s"Batching pagination: $pg")
            val stmt = generatePaginationStatement(sourceConn.conn, querySqlCst(lastUpdateTime), pg)
            val df   = conversionFn(sourceHelper.readTable(stmt))

            targetHelper.saveTable(df, targetConn.table, SaveMode.Append)
            log.info(s"Batching $idx saved complete")
          }
          log.info("BatchInsert complete!")
      }
      log.info("InsertFromLastUpdateTime complete!")
  }

  def insertFromLastUpdateTime(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      querySqlCst: String => String,
      batchOption: Option[BatchOption]
  )(implicit spark: SparkSession): Unit =
    insertFromLastUpdateTime(sourceConn, targetConn, querySqlCst, batchOption, df => df)

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
      batchOption: Option[BatchOption],
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = lastUpdateTimeCurrying(sourceConn, targetConn) {
    (
        sourceHelper,
        targetHelper,
        lastUpdateTime
    ) =>
      log.info("Starting UpsertFromLastUpdateTime...")
      batchOption match {
        case None =>
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
        case Some(bo) =>
          log.info("Starting BatchUpsert...")
          bo.genIterPagination.zipWithIndex.foreach { case (pg, idx) =>
            log.info(s"Batching num: $idx")
            log.info(s"Batching pagination: $pg")
            val stmt = generatePaginationStatement(sourceConn.conn, querySqlCst(lastUpdateTime), pg)
            val df   = conversionFn(sourceHelper.readTable(stmt))

            targetHelper.upsertTable(
              df,
              targetConn.table,
              None,
              false,
              onConflictColumns,
              RegimeJdbcHelper.UpsertAction.DoUpdate
            )
            log.info(s"Batching $idx saved complete")
          }
          log.info("BatchUpsert complete!")
      }
      log.info("UpsertFromLastUpdateTime complete!")
  }

  def upsertFromLastUpdateTime(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      onConflictColumns: Seq[String],
      batchOption: Option[BatchOption],
      querySqlCst: String => String
  )(implicit spark: SparkSession): Unit =
    upsertFromLastUpdateTime(
      sourceConn,
      targetConn,
      onConflictColumns,
      querySqlCst,
      batchOption,
      df => df
    )
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
    fetchSize: Int,
    callingTimes: Int
) {

  private def genPagination(i: Int): Pagination = {
    Pagination(orderBy, isAsc, fetchSize, fetchSize * i)
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
      fetchSize: Int,
      callingTimes: Int
  ): Option[BatchOption] = {
    if (fetchSize < 0 || callingTimes < 0) {
      None
    } else {
      Some(BatchOption(orderBy, isAsc, fetchSize, callingTimes))
    }
  }
}
