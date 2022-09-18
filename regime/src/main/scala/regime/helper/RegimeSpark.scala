package regime.helper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import regime.Conn
import regime.ConnTableColumn
import regime.ConnTable

trait RegimeSpark {

  // logging
  val log = LogManager.getRootLogger
  log.setLevel(Level.INFO)

  // ===============================================================================================
  // Abstract attributes & functions
  // ===============================================================================================

  val appName: String

  def process(args: String*)(implicit spark: SparkSession): Unit

  def finish(args: String*)(implicit sparkBuilder: SparkSession.Builder): Unit = {
    log.info(s"args: $args")
    implicit val spark = sparkBuilder.appName(appName).getOrCreate()
    process(args: _*)
    spark.stop()
  }

  // ===============================================================================================
  // Execute functions
  // ===============================================================================================

  /** Sync all data from one table to another.
    *
    * Only works for the first time.
    *
    * @param from
    * @param to
    * @param sql
    * @param batchOption
    * @param conversionFn
    * @param spark
    */
  def syncInitAll(
      from: ConnTable,
      to: ConnTable,
      sql: String,
      batchOption: Option[BatchOption],
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a SyncInitAll task...")
    val helper = RegimeJdbcHelper(to.conn)
    val saveTo = to.table

    log.info("Checking if table exists...")
    if (helper.tableExists(saveTo))
      throw new Exception(s"Table $saveTo already exists, SyncAll operation is not allowed!")

    try {
      batchOption match {
        case None =>
          log.info("Loading data into memory...")
          val df = conversionFn(RegimeJdbcHelper(from.conn).readTable(sql))

          log.info("Writing data into database...")
          helper.saveTable(df, saveTo, SaveMode.ErrorIfExists)

          log.info("Writing process complete!")
        case Some(bo) =>
          RegimeSyncHelper.batchInsert(
            from,
            to,
            sql,
            bo,
            conversionFn
          )
      }
    } catch {
      case e: Throwable =>
        log.error(e)
        log.error(
          s"""
          Input parameters:
          from: $from
          sql: $sql
          to: $to
          batchOption: $batchOption
          conversionFn: $conversionFn
          """
        )
        sys.exit()
    }

    log.info("SyncInitAll task complete!")
  }

  def syncInitAll(
      from: ConnTable,
      to: ConnTable,
      sql: String,
      batchOption: Option[BatchOption]
  )(implicit spark: SparkSession): Unit = syncInitAll(from, to, sql, batchOption, df => df)

  /** Replace all data from one table.
    *
    * All the current existing data will be dumped.
    *
    * @param from
    * @param to
    * @param sql
    * @param batchOption
    * @param conversionFn
    * @param spark
    */
  def syncReplaceAll(
      from: ConnTable,
      to: ConnTable,
      sql: String,
      batchOption: Option[BatchOption],
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a SyncReplaceAll task...")
    val helper = RegimeJdbcHelper(to.conn)
    val saveTo = to.table

    try {
      batchOption match {
        case None =>
          log.info("Loading data into memory...")
          val df = conversionFn(RegimeJdbcHelper(from.conn).readTable(sql))

          log.info("Writing data into database...")
          helper.saveTable(df, saveTo, SaveMode.Overwrite)

          log.info("Writing process complete!")
        case Some(bo) =>
          helper.truncateTable(saveTo, false)
          RegimeSyncHelper.batchInsert(
            from,
            to,
            sql,
            bo,
            conversionFn
          )
      }
    } catch {
      case e: Throwable =>
        log.error(e)
        log.error(
          s"""
          Input parameters:
          from: $from
          sql: $sql
          to: $to
          batchOption: $batchOption
          conversionFn: $conversionFn
          """
        )
        sys.exit()
    }

    log.info("SyncReplaceAll task complete!")
  }

  def syncReplaceAll(
      from: ConnTable,
      to: ConnTable,
      sql: String,
      batchOption: Option[BatchOption]
  )(implicit spark: SparkSession): Unit = syncReplaceAll(from, to, sql, batchOption, df => df)

  /** Sync data from one table to another by upsert.
    *
    * @param from
    * @param to
    * @param sql
    * @param onConflictColumns
    * @param batchOption
    * @param conversionFn
    * @param spark
    */
  def syncUpsert(
      from: ConnTable,
      to: ConnTable,
      sql: String,
      onConflictColumns: Seq[String],
      batchOption: Option[BatchOption],
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a SyncUpsert task...")
    val helper = RegimeJdbcHelper(to.conn)
    val saveTo = to.table

    log.info("Checking if table exists...")
    if (!helper.tableExists(saveTo))
      throw new Exception(s"Table $saveTo doesn't exists, SyncUpsert operation is not all allowed!")

    try {
      batchOption match {
        case None =>
          val df = conversionFn(RegimeJdbcHelper(from.conn).readTable(sql))

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
        case Some(bo) =>
          RegimeSyncHelper.batchUpsert(
            from,
            to,
            onConflictColumns,
            sql,
            bo,
            conversionFn
          )
      }
    } catch {
      case e: Throwable =>
        log.error(e)
        log.error(
          s"""
          Input parameters:
          from: $from
          sql: $sql
          to: $to
          onConflictColumns: $onConflictColumns
          batchOption: $batchOption
          conversionFn: $conversionFn
          """
        )
        sys.exit()
    }

    log.info("SyncUpsert task complete!")
  }

  def syncUpsert(
      from: ConnTable,
      to: ConnTable,
      sql: String,
      onConflictColumns: Seq[String],
      batchOption: Option[BatchOption]
  )(implicit spark: SparkSession): Unit =
    syncUpsert(from, to, sql, onConflictColumns, batchOption, df => df)

  /** Sync and insert data from the last update point.
    *
    * @param from
    * @param to
    * @param querySqlCst
    * @param batchOption
    * @param timeCvtFn
    * @param conversionFn
    * @param spark
    */
  def syncInsertFromLastUpdate(
      from: ConnTableColumn,
      to: ConnTableColumn,
      querySqlCst: String => String,
      batchOption: Option[BatchOption],
      timeCvtFn: Option[DataFrame => DataFrame],
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a SyncInsertFromLastUpdate...")
    RegimeSyncHelper.insertFromLastUpdateTime(
      from,
      to,
      querySqlCst,
      batchOption,
      timeCvtFn,
      conversionFn
    )
    log.info("SyncInsertFromLastUpdate task complete!")
  }

  def syncInsertFromLastUpdate(
      from: ConnTableColumn,
      to: ConnTableColumn,
      querySqlCst: String => String,
      batchOption: Option[BatchOption],
      timeCvtFn: Option[DataFrame => DataFrame]
  )(implicit spark: SparkSession): Unit =
    syncInsertFromLastUpdate(from, to, querySqlCst, batchOption, timeCvtFn, df => df)

  /** Sync and upsert data from the last update point.
    *
    * This is very useful when the source data has been changed. If the source data has some records
    * been deleted, then removing the whole target data who matches the conditions and calling
    * insert again is a better idea.
    *
    * @param from
    * @param to
    * @param onConflictColumns
    * @param querySqlCst
    * @param batchOption
    * @param timeCvtFn
    * @param conversionFn
    * @param spark
    */
  def syncUpsertFromLastUpdate(
      from: ConnTableColumn,
      to: ConnTableColumn,
      onConflictColumns: Seq[String],
      querySqlCst: String => String,
      batchOption: Option[BatchOption],
      timeCvtFn: Option[DataFrame => DataFrame],
      conversionFn: DataFrame => DataFrame
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a SyncUpsertFromLastUpdate...")
    RegimeSyncHelper.upsertFromLastUpdateTime(
      from,
      to,
      onConflictColumns,
      querySqlCst,
      batchOption,
      timeCvtFn,
      conversionFn
    )
    log.info("SyncUpsertFromLastUpdate task complete!")
  }

  def syncUpsertFromLastUpdate(
      from: ConnTableColumn,
      to: ConnTableColumn,
      onConflictColumns: Seq[String],
      querySqlCst: String => String,
      batchOption: Option[BatchOption],
      timeCvtFn: Option[DataFrame => DataFrame]
  )(implicit spark: SparkSession): Unit =
    syncUpsertFromLastUpdate(
      from,
      to,
      onConflictColumns,
      querySqlCst,
      batchOption,
      timeCvtFn,
      df => df
    )

  /** Clean null data
    *
    * @param ct
    * @param columns
    * @param conjunction
    * @param spark
    */
  def cleanNullData(
      ct: ConnTable,
      columns: Seq[String],
      conjunction: RegimeJdbcHelper.Conjunction.Value
  )(implicit spark: SparkSession): Unit = {
    log.info("Starting a CleanNullData task...")
    RegimeJdbcHelper(ct.conn).deleteNullValues(
      ct.table,
      columns,
      conjunction,
      res => log.info(s"Deleted $res rows")
    )
    log.info("CleanNullData task complete!")
  }

  def cleanNullData(
      ct: ConnTable,
      columns: Seq[String],
      conjunction: String
  )(implicit spark: SparkSession): Unit = {
    val cj = conjunction match {
      case "and" => RegimeJdbcHelper.Conjunction.AND
      case "or"  => RegimeJdbcHelper.Conjunction.OR
    }
    cleanNullData(ct, columns, cj)
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
