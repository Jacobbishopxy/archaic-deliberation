package regime.market

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.util.SizeEstimator

import regime.helper._
import regime.Conn
import regime.ConnTableColumn
import regime.ConnTable

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
    * @param spark
    */
  def syncAll(
      from: Conn,
      sql: String,
      to: ConnTable
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a SyncAll task...")
    log.info("Checking if table exists...")
    val helper = RegimeJdbcHelper(to.conn)
    val saveTo = to.table
    if (helper.tableExists(saveTo))
      throw new Exception(s"Table $saveTo already exists, SyncAll operation is not allowed!")

    log.info("Loading data into memory...")

    val df = RegimeJdbcHelper(from).readTable(sql)

    log.info(s"Size estimate: ${SizeEstimator.estimate(df)}")
    log.info("Writing data into database...")

    helper.saveTable(df, saveTo, SaveMode.ErrorIfExists)

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
      to: ConnTable,
      onConflictColumns: Seq[String]
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a SyncUpsert task...")
    log.info("Checking if table exists...")
    val saveTo = to.table
    val helper = RegimeJdbcHelper(to.conn)
    if (!helper.tableExists(saveTo))
      throw new Exception(s"Table $saveTo doesn't exists, SyncUpsert operation is not all allowed!")

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

    helper.upsertTable(
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

  /** Sync data from the last update point.
    *
    * @param from
    * @param to
    * @param querySqlCst
    * @param spark
    */
  def syncInsertFromLastUpdate(
      from: ConnTableColumn,
      to: ConnTableColumn,
      querySqlCst: Any => String
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a SyncInsertFromLastUpdate...")
    val size = RegimeTimeHelper.insertFromLastUpdateTime(
      from,
      to,
      querySqlCst
    )

    log.info(s"Size estimate: ${size}")
    log.info("SyncInsertFromLastUpdate task complete!")
  }

  // ===============================================================================================
  // ExecuteOnce functions
  // ===============================================================================================

  /** Create primary key
    *
    * @param connTable
    * @param primaryKeyName
    * @param primaryColumn
    * @param spark
    */
  def createPrimaryKey(
      connTable: ConnTable,
      primaryKeyName: String,
      primaryColumn: Seq[String]
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a CreatePrimaryKey task...")
    log.info(
      s"""
      conn: ${connTable.conn}
      table: ${connTable.table}
      primaryKeyName: $primaryKeyName
      primaryColumn: $primaryColumn
      """
    )

    RegimeJdbcHelper(connTable.conn).createPrimaryKey(
      connTable.table,
      primaryKeyName,
      primaryColumn
    )

    log.info("CreatePrimaryKey task complete!")
  }

  /** Create indexes
    *
    * @param connTable
    * @param indexes
    * @param spark
    */
  def createIndexes(
      connTable: ConnTable,
      indexes: Seq[(String, Seq[String])]
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a CreateIndexes task...")
    log.info(
      s"""
      conn: ${connTable.conn}
      table: ${connTable.table}
      indexes: $indexes
      """
    )
    val helper = RegimeJdbcHelper(connTable.conn)

    indexes.foreach(ele => helper.createIndex(connTable.table, ele._1, ele._2))

    log.info("CreateIndexes task complete!")
  }

  /** Create primary key and index
    *
    * @param connTable
    * @param primaryKey
    * @param indexes
    * @param spark
    */
  def createPrimaryKeyAndIndex(
      connTable: ConnTable,
      primaryKey: (String, Seq[String]),
      indexes: Seq[(String, Seq[String])]
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a CreatePrimaryKeyAndIndex task...")
    log.info(
      s"""
      conn: ${connTable.conn}
      table: ${connTable.table}
      primaryKey: $primaryKey
      indexes: $indexes
      """
    )
    val helper = RegimeJdbcHelper(connTable.conn)

    // primary key
    helper.createPrimaryKey(connTable.table, primaryKey._1, primaryKey._2)

    // indexes
    indexes.foreach(ele => helper.createIndex(connTable.table, ele._1, ele._2))

    log.info("CreatePrimaryKeyAndIndex task complete!")
  }

}
