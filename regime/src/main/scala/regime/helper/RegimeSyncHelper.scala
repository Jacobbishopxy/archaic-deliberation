package regime.helper

import scala.util.Try
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{TimestampType}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import regime.{Conn, ConnTable, ConnTableColumn, DriverType}

/** Get the latest update time from the target table, and query the rest of data from the resource
  * table.
  */
object RegimeSyncHelper {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  // ===============================================================================================
  // private helper functions
  // ===============================================================================================

  /** A curry function used by all the other Sync functions.
    *
    * Get the last update time from the target data if the source data is heading forward.
    *
    * @param sourceConn
    * @param targetConn
    * @param timeCvtFn
    * @param fn
    * @param spark
    */
  private def lastUpdateTimeCurrying(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      timeCvtFn: Option[DataFrame => DataFrame]
  )(
      fn: (RegimeJdbcHelper, RegimeJdbcHelper, String) => Unit
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting get last update time...")
    // helpers
    val sourceHelper = RegimeJdbcHelper(sourceConn.conn)
    val targetHelper = RegimeJdbcHelper(targetConn.conn)

    // get latest date from both tables
    val sourceDf = sourceHelper.readTable(
      RegimeSqlHelper.generateGetMaxValue(sourceConn.table, sourceConn.column)
    )
    val rawTargetDf = targetHelper.readTable(
      RegimeSqlHelper.generateGetMaxValue(targetConn.table, targetConn.column)
    )
    val targetDf = timeCvtFn.fold(rawTargetDf)(_(rawTargetDf))

    // get the lesser value from target table if exists,
    // and if value exists, execute `fn`
    RegimeDFHelper.checkIfTargetValueIsLesser(sourceDf, targetDf).map { lv =>
      log.info(s"Last update time is $lv")
      fn(sourceHelper, targetHelper, lv.toString)
    }
  }

  // ===============================================================================================
  // general functions
  // 1. generateBatchOption
  // 1. batchInsert
  // 1. batchUpsert
  // 1. insertFromLastUpdateTime
  // 1. upsertFromLastUpdateTime
  // 1. replaceAllIfLastUpdateTimeChanged
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
    val sql    = RegimeSqlHelper.generateCountFromStatement(ctc.column, ctc.table)

    val tableHead = helper.readTable(sql).first()
    // In case of different type of value
    val callingTimes = Try {
      math.floor(tableHead.getLong(0) / fetchSize).toInt
    } orElse {
      Try(math.floor(tableHead.getInt(0) / fetchSize).toInt)
    } getOrElse {
      throw new ClassCastException("Invalid counting type")
    }

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
      val stmt = RegimeSqlHelper.generatePaginationStatement(sourceConn.conn, sql, pg)
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
      val stmt = RegimeSqlHelper.generatePaginationStatement(sourceConn.conn, sql, pg)
      val df   = conversionFn(sourceHelper.readTable(stmt))

      targetHelper.upsertTable(
        df,
        targetConn.table,
        None,
        false,
        onConflictColumns,
        UpsertAction.DoUpdate
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
      timeCvtFn: Option[DataFrame => DataFrame],
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit =
    lastUpdateTimeCurrying(sourceConn, targetConn, timeCvtFn) {
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
              val stmt = RegimeSqlHelper.generatePaginationStatement(
                sourceConn.conn,
                querySqlCst(lastUpdateTime),
                pg
              )
              val df = conversionFn(sourceHelper.readTable(stmt))

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
      batchOption: Option[BatchOption],
      timeCvtFn: Option[DataFrame => DataFrame]
  )(implicit spark: SparkSession): Unit =
    insertFromLastUpdateTime(sourceConn, targetConn, querySqlCst, batchOption, timeCvtFn, df => df)

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
      timeCvtFn: Option[DataFrame => DataFrame],
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit =
    lastUpdateTimeCurrying(sourceConn, targetConn, timeCvtFn) {
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
              UpsertAction.DoUpdate
            )
          case Some(bo) =>
            log.info("Starting BatchUpsert...")
            bo.genIterPagination.zipWithIndex.foreach { case (pg, idx) =>
              log.info(s"Batching num: $idx")
              log.info(s"Batching pagination: $pg")
              val stmt = RegimeSqlHelper.generatePaginationStatement(
                sourceConn.conn,
                querySqlCst(lastUpdateTime),
                pg
              )
              val df = conversionFn(sourceHelper.readTable(stmt))

              targetHelper.upsertTable(
                df,
                targetConn.table,
                None,
                false,
                onConflictColumns,
                UpsertAction.DoUpdate
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
      querySqlCst: String => String,
      batchOption: Option[BatchOption],
      timeCvtFn: Option[DataFrame => DataFrame]
  )(implicit spark: SparkSession): Unit =
    upsertFromLastUpdateTime(
      sourceConn,
      targetConn,
      onConflictColumns,
      querySqlCst,
      batchOption,
      timeCvtFn,
      df => df
    )

  /** If last update date has changed, then replaceAll data
    *
    * @param sourceConn
    * @param targetConn
    * @param querySql
    * @param timeCvtFn
    * @param conversionFn
    * @param spark
    */
  def replaceAllIfLastUpdateTimeChanged(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      querySql: String,
      timeCvtFn: Option[DataFrame => DataFrame],
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit =
    lastUpdateTimeCurrying(sourceConn, targetConn, timeCvtFn) {
      (
          sourceHelper,
          targetHelper,
          lastUpdateTime
      ) =>
        log.info("Starting ReplaceAllIfLastUpdateTimeChanged...")

        // Read all
        val df = conversionFn(sourceHelper.readTable(querySql))
        // Truncate table
        targetHelper.truncateTable(targetConn.table, false)
        // Save all
        targetHelper.saveTable(df, targetConn.table, SaveMode.Append)

        log.info("ReplaceAllIfLastUpdateTimeChanged complete!")
    }

  def replaceAllIfLastUpdateTimeChanged(
      sourceConn: ConnTableColumn,
      targetConn: ConnTableColumn,
      querySql: String,
      timeCvtFn: Option[DataFrame => DataFrame]
  )(implicit spark: SparkSession): Unit =
    replaceAllIfLastUpdateTimeChanged(
      sourceConn,
      targetConn,
      querySql,
      timeCvtFn,
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
