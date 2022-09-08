package regime.market

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import regime.helper._
import regime.Conn
import org.apache.spark.util.SizeEstimator

trait RegimeTask extends RegimeSpark {

  // ===============================================================================================
  // Execute functions
  // ===============================================================================================

  /** Sync all data from one table to another.
    *
    * Make sure OOM issue when syncing a big table.
    *
    * @param from
    * @param sql
    * @param to
    * @param saveTo
    * @param spark
    */
  def syncAll(
      from: Conn,
      sql: String,
      to: Conn,
      saveTo: String
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a SyncAll task...")
    log.info("Loading data into memory...")

    val df = RegimeJdbcHelper(from).readTable(sql)

    log.info(s"Size estimate: ${SizeEstimator.estimate(df)}")
    log.info("Writing data into database...")

    RegimeJdbcHelper(to).saveTable(df, saveTo, SaveMode.Overwrite)

    log.info("Writing process complete!")
    log.info("SyncAll task complete!")
  }

  /** Sync data from one table to another by upsert.
    *
    * @param from
    * @param sql
    * @param to
    * @param onConflictColumns
    * @param saveTo
    * @param spark
    */
  def syncUpsert(
      from: Conn,
      sql: String,
      to: Conn,
      onConflictColumns: Seq[String],
      saveTo: String
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a SyncUpsert task...")
    log.info(
      s"""
      from: $from
      sql: $sql
      to: $to
      onConflictColumns: $onConflictColumns
      saveTo: $saveTo
      """
    )

    val df = RegimeJdbcHelper(from).readTable(sql)

    log.info(s"Size estimate: ${SizeEstimator.estimate(df)}")
    log.info("Writing data into database...")

    RegimeJdbcHelper(to).upsertTable(
      df,
      saveTo,
      None,
      false,
      onConflictColumns,
      RegimeJdbcHelper.UpsertAction.DoUpdate
    )

    log.info("Writing process complete!")
    log.info("SyncAll task complete!")
  }

  // ===============================================================================================
  // ExecuteOnce functions
  // ===============================================================================================

  def createPrimaryKey(
      conn: Conn,
      table: String,
      primaryKeyName: String,
      primaryColumn: Seq[String]
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a CreatePrimaryKey task...")
    log.info(
      s"""
      conn: $conn
      table: $table
      primaryKeyName: $primaryKeyName
      primaryColumn: $primaryColumn
      """
    )

    RegimeJdbcHelper(conn).createPrimaryKey(table, primaryKeyName, primaryColumn)

    log.info("CreatePrimaryKey task complete!")
  }

  def createIndexes(
      conn: Conn,
      table: String,
      indexes: Seq[(String, Seq[String])]
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a CreateIndexes task...")
    log.info(
      s"""
      conn: $conn
      table: $table
      indexes: $indexes
      """
    )
    val helper = RegimeJdbcHelper(conn)

    indexes.foreach(ele => helper.createIndex(table, ele._1, ele._2))

    log.info("CreateIndexes task complete!")
  }

  def createPrimaryKeyAndIndex(
      conn: Conn,
      table: String,
      primaryKey: (String, Seq[String]),
      indexes: Seq[(String, Seq[String])]
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a CreatePrimaryKeyAndIndex task...")
    log.info(
      s"""
      conn: $conn
      table: $table
      primaryKey: $primaryKey
      indexes: $indexes
      """
    )
    val helper = RegimeJdbcHelper(conn)

    // primary key
    helper.createPrimaryKey(table, primaryKey._1, primaryKey._2)

    // indexes
    indexes.foreach(ele => helper.createIndex(table, ele._1, ele._2))

    log.info("CreatePrimaryKeyAndIndex task complete!")
  }

}
