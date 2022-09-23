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

object RegimeCalcHelper {

  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  // ===============================================================================================
  // private helper functions
  // ===============================================================================================

  /** A curry function used by all the other Sync functions.
    *
    * Get the last update time from the target data if the source data is heading forward.
    *
    * Note: different from `RegimeSyncHelper`, this method only works in the same source
    *
    * @param conn
    * @param sourceTableColumn
    * @param targetTableColumn
    * @param timeCvtFn
    * @param fn
    * @param spark
    */
  private def lastUpdateTimeCurrying(
      conn: Conn,
      sourceTableColumn: (String, String),
      targetTableColumn: (String, String),
      timeCvtFn: Option[DataFrame => DataFrame]
  )(
      fn: (RegimeJdbcHelper, String) => Unit
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting get last update time...")
    val helper = RegimeJdbcHelper(conn)

    // get latest date from both tables
    val sourceDf = helper.readTable(
      RegimeSqlHelper.generateGetMaxValue(sourceTableColumn._1, sourceTableColumn._2)
    )
    val rawTargetDf = helper.readTable(
      RegimeSqlHelper.generateGetMaxValue(targetTableColumn._1, targetTableColumn._2)
    )
    val targetDf = timeCvtFn.fold(rawTargetDf)(_(rawTargetDf))

    // get the lesser value from target table if exists,
    // and if value exists, execute `fn`
    RegimeDFHelper.checkIfTargetValueIsLesser(sourceDf, targetDf).map { lv =>
      fn(helper, lv.toString)
    }
  }

  // ===============================================================================================
  // general functions
  // 1. insertFromLastUpdateTime
  // 1. upsertFromLastUpdateTime
  // ===============================================================================================

  /** Insert from last update time.
    *
    * @param conn
    * @param sourceTableColumn
    * @param targetTableColumn
    * @param timeCvtFn
    * @param dfProducer
    * @param spark
    */
  def insertFromLastUpdateTime(
      conn: Conn,
      sourceTableColumn: (String, String),
      targetTableColumn: (String, String),
      timeCvtFn: Option[DataFrame => DataFrame],
      dfProducer: (RegimeJdbcHelper, String) => DataFrame
  )(implicit spark: SparkSession): Unit =
    lastUpdateTimeCurrying(conn, sourceTableColumn, targetTableColumn, timeCvtFn) {
      (helper, lastUpdateTime) =>
        log.info("Starting InsertFromLastUpdateTime...")

        val df = dfProducer(helper, lastUpdateTime)

        helper.saveTable(df, targetTableColumn._1, SaveMode.Append)

        log.info("InsertFromLastUpdateTime complete!")
    }

  /** Upsert from last update time.
    *
    * @param conn
    * @param sourceTableColumn
    * @param targetTableColumn
    * @param onConflictColumns
    * @param timeCvtFn
    * @param dfProducer
    * @param spark
    */
  def upsertFromLastUpdateTime(
      conn: Conn,
      sourceTableColumn: (String, String),
      targetTableColumn: (String, String),
      onConflictColumns: Seq[String],
      timeCvtFn: Option[DataFrame => DataFrame],
      dfProducer: (RegimeJdbcHelper, String) => DataFrame
  )(implicit spark: SparkSession): Unit =
    lastUpdateTimeCurrying(conn, sourceTableColumn, targetTableColumn, timeCvtFn) {
      (helper, lastUpdateTime) =>
        log.info("Starting UpsertFromLastUpdateTime...")

        val df = dfProducer(helper, lastUpdateTime)

        helper.upsertTable(
          df,
          targetTableColumn._1,
          None,
          false,
          onConflictColumns,
          UpsertAction.DoUpdate
        )

        log.info("UpsertFromLastUpdateTime complete!")
    }
}
