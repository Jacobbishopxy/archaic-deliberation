package regime.helper

import scala.io.Source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

import regime.{Conn, ConnTable, ConnTableColumn, DriverType, Global}

object RegimeSqlHelper {

  // ===============================================================================================
  // SQL string read
  // ===============================================================================================

  def fromFile(filepath: String): String =
    Source.fromFile(filepath).mkString

  def fromResource(filename: String): String =
    Source.fromResource(filename).mkString

  // ===============================================================================================
  // General SQL statement
  // ===============================================================================================

  /** Unsupported driver announcement
    *
    * @param method
    * @return
    */
  private def genUnsupportedDriver(method: String): UnsupportedOperationException =
    new UnsupportedOperationException(
      s"Unsupported driver, $method method only works on: org.postgresql.Driver/com.mysql.cj.jdbc.Driver"
    )

  /** Generate a create primary key SQL statement.
    *
    * Currently, only supports MySQL & PostgreSQL
    *
    * @param conn
    * @param tableName
    * @param primaryKeyName
    * @param columns
    * @return
    */
  def generateCreatePrimaryKeyStatement(
      conn: Conn,
      tableName: String,
      primaryKeyName: String,
      columns: Seq[String]
  ): String = conn.driverType match {
    case DriverType.Postgres =>
      s"""
      ALTER TABLE $tableName
      ADD CONSTRAINT $primaryKeyName
      PRIMARY KEY (${columns.mkString(",")})
      """
    case DriverType.MySql =>
      s"""
      ALTER TABLE $tableName
      ADD PRIMARY KEY (${columns.mkString(",")})
      """
    case _ =>
      throw genUnsupportedDriver("createPrimaryKey")
  }

  /** Generate a drop primary key SQL statement.
    *
    * Currently, only supports MySQL & PostgreSQL
    *
    * @param conn
    * @param tableName
    * @param primaryKeyName
    * @return
    */
  def generateDropPrimaryKeyStatement(
      conn: Conn,
      tableName: String,
      primaryKeyName: String
  ): String = {
    conn.driverType match {
      case DriverType.Postgres =>
        s"""
        ALTER TABLE $tableName
        DROP CONSTRAINT $primaryKeyName
        """
      case DriverType.MySql =>
        s"""
        ALTER TABLE $tableName
        DROP PRIMARY KEY
        """
    }
  }

  /** Generate an upsert SQL statement.
    *
    * Currently, only supports MySQL & PostgreSQL
    *
    * @param conn
    * @param table
    * @param tableSchema
    * @param conditions
    * @param isCaseSensitive
    * @param conflictColumns
    * @param conflictAction
    * @return
    */
  def generateUpsertStatement(
      conn: Conn,
      table: String,
      tableSchema: StructType,
      conditions: Option[String],
      isCaseSensitive: Boolean,
      conflictColumns: Seq[String],
      conflictAction: UpsertAction.Value
  ): String = {
    val columnNameEquality = if (isCaseSensitive) {
      org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
    } else {
      org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
    }
    val tableColumnNames  = tableSchema.fieldNames
    val tableSchemaFields = tableSchema.fields
    val dialect           = JdbcDialects.get(conn.url)

    val columns = tableSchemaFields
      .map { col =>
        val normalizedName = tableColumnNames
          .find(columnNameEquality(_, col.name))
          .get
        dialect.quoteIdentifier(normalizedName)
      }
      .mkString(",")

    val conflictString = conflictColumns
      .map(x => s""""$x"""")
      .mkString(",")
    val placeholders = tableSchemaFields
      .map(_ => "?")
      .mkString(",")

    val stmt = (conn.driverType, conflictAction) match {
      case (DriverType.Postgres, UpsertAction.DoUpdate) =>
        val updateSet = tableColumnNames
          .filterNot(conflictColumns.contains(_))
          .map(n => s""""$n" = EXCLUDED."$n"""")
          .mkString(",")

        s"""
        INSERT INTO
          $table ($columns)
        VALUES
          ($placeholders)
        ON CONFLICT
          ($conflictString)
        DO UPDATE SET
          $updateSet
        """
      case (DriverType.Postgres, _) =>
        s"""
        INSERT INTO
          $table ($columns)
        VALUES
          ($placeholders)
        ON CONFLICT
          ($conflictString)
        DO NOTHING
        """
      case (DriverType.MySql, UpsertAction.DoUpdate) =>
        val updateSet = tableColumnNames
          .filterNot(conflictColumns.contains(_))
          .map(n => s""""$n" = VALUES("$n")""")
          .mkString(",")

        s"""
        INSERT INTO
          $table ($columns)
        VALUES
          ($placeholders)
        ON DUPLICATE KEY UPDATE
          $updateSet
        """
      case (DriverType.MySql, _) =>
        val updateSet = tableColumnNames
          .filterNot(conflictColumns.contains(_))
          .map(n => s""""$n" = "$n"""")
          .mkString(",")

        s"""
        INSERT INTO
          $table ($columns)
        VALUES
          ($placeholders)
        ON DUPLICATE KEY UPDATE
          $updateSet
        """
      case _ =>
        throw genUnsupportedDriver("upsert")
    }

    conditions match {
      case Some(value) => stmt ++ s"\nWHERE $value"
      case None        => stmt
    }
  }

  /** Generate a createIndex SQL statement
    *
    * @param conn
    * @param table
    * @param name
    * @param columns
    * @return
    */
  def generateCreateIndexStatement(
      conn: Conn,
      table: String,
      indexName: String,
      columns: Seq[String]
  ): String =
    conn.driverType match {
      case DriverType.Postgres =>
        val cl = columns.map(c => s""""$c"""").mkString(",")
        s"""
        CREATE INDEX "$indexName" ON "$table" ($cl)
        """
      case DriverType.MySql =>
        val cl = columns.map(c => s"""`$c`""").mkString(",")
        s"""
        CREATE INDEX `$indexName` ON `$table` ($cl)
        """
      case _ =>
        throw genUnsupportedDriver("createIndex")
    }

  /** Generate a dropIndex SQL statement
    *
    * @param conn
    * @param table
    * @param name
    * @return
    */
  def generateDropIndexStatement(
      conn: Conn,
      table: String,
      name: String
  ): String =
    conn.driverType match {
      case DriverType.Postgres =>
        s"""
        DROP INDEX "$name"
        """
      case DriverType.MySql =>
        s"""
        DROP INDEX `$name` ON `$table`
        """
      case _ =>
        throw genUnsupportedDriver("dropIndex")
    }

  /** Generate a createForeignKey SQL statement
    *
    * @param conn
    * @param fromTable
    * @param fromTableColumn
    * @param foreignKeyName
    * @param toTable
    * @param toTableColumn
    * @param onDelete
    * @param onUpdate
    * @return
    */
  def generateCreateForeignKey(
      conn: Conn,
      fromTable: String,
      fromTableColumn: String,
      foreignKeyName: String,
      toTable: String,
      toTableColumn: String,
      onDelete: Option[ForeignKeyModifyAction.Value],
      onUpdate: Option[ForeignKeyModifyAction.Value]
  ): String =
    conn.driverType match {
      case DriverType.Postgres =>
        s"""
        ALTER TABLE "$fromTable"
        ADD CONSTRAINT "$foreignKeyName"
        FOREIGN KEY ("$fromTableColumn")
        REFERENCES "$toTable" ("$toTableColumn")
        """ +
          onDelete
            .map(ForeignKeyModifyAction.generateString(_, DeleteOrUpdate.Delete))
            .getOrElse("") +
          onUpdate
            .map(ForeignKeyModifyAction.generateString(_, DeleteOrUpdate.Update))
            .getOrElse("")

      case DriverType.MySql =>
        s"""
        ALTER TABLE `$fromTable`
        ADD CONSTRAINT `$foreignKeyName`
        FOREIGN KEY (`$fromTableColumn`)
        REFERENCES `$toTable` (`$toTableColumn`)
        """ +
          onDelete
            .map(ForeignKeyModifyAction.generateString(_, DeleteOrUpdate.Delete))
            .getOrElse("") +
          onUpdate
            .map(ForeignKeyModifyAction.generateString(_, DeleteOrUpdate.Update))
            .getOrElse("")
      case _ =>
        throw genUnsupportedDriver("createForeignKey")
    }

  /** Generate a dropForeignKey SQL statement
    *
    * @param conn
    * @param table
    * @param foreignKeyName
    * @return
    */
  def generateDropForeignKey(
      conn: Conn,
      table: String,
      foreignKeyName: String
  ): String = {
    conn.driverType match {
      case DriverType.Postgres =>
        s"""
        ALTER TABLE "$table" DROP CONSTRAINT "$foreignKeyName"
        """
      case DriverType.MySql =>
        s"""
        ALTER TABLE `$table` DROP FOREIGN KEY "$foreignKeyName"
        """
      case _ =>
        throw genUnsupportedDriver("dropForeignKey")
    }
  }

  private def orderDir(isAsc: Boolean): String =
    if (isAsc) "ASC" else "DESC"

  /** Generate a pagination statement
    *
    * @param conn
    * @param sql
    * @param pagination
    * @return
    */
  def generatePaginationStatement(
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

  /** Count from table statement
    *
    * @param column
    * @param table
    * @return
    */
  def generateCountFromStatement(column: String, table: String): String =
    s"""
    SELECT COUNT(${column}) AS count FROM ${table}
    """

  /** Get max value statement
    *
    * @param tableName
    * @param columnName
    * @return
    */
  def generateGetMaxValue(tableName: String, columnName: String) = s"""
    SELECT MAX($columnName) AS max_$columnName FROM $tableName
    """

  /** Add queryFromDate where clause to a SQL statement
    *
    * @param query
    * @param column
    * @param date
    * @return
    */
  def generateQueryFromDate(query: String, column: String, date: String) = s"""
    $query
    WHERE $column > '$date'
    """

  /** Add queryBetweenDate where clause to a SQL statement
    *
    * @param query
    * @param column
    * @param dateRange
    * @return
    */
  def generateQueryDateRange(query: String, column: String, dateRange: (String, String)) = s"""
    $query
    WHERE $column BETWEEN '${dateRange._1}' AND '${dateRange._2}'
    """

  /** Add queryAtDate where clause to a SQL statement
    *
    * @param query
    * @param column
    * @param date
    * @return
    */
  def generateQueryAtDate(query: String, column: String, date: String) = s"""
    $query
    WHERE $column = '$date'
    """

  // ===============================================================================================
  // Spark SQL
  // ===============================================================================================

  /** first_value()
    *
    * @param tableName
    * @param columnName
    * @return
    */
  def generateGetFirstValue(tableName: String, columnName: String) = s"""
    SELECT first_value(max_$columnName) FROM $tableName
    """

}

/** Upsert action's enum
  */
object UpsertAction extends Enumeration {
  type UpsertAction = Value
  val DoNothing, DoUpdate = Value
}

/** OnDelete/OnUpdate. Private marker
  */
private object DeleteOrUpdate extends Enumeration {
  type DeleteOrUpdate = Value
  val Delete, Update = Value

  def generateString(v: Value): String = v match {
    case Delete => "DELETE"
    case Update => "UPDATE"
  }
}

/** ForeignKey onDelete/onUpdate's actions
  */
object ForeignKeyModifyAction extends Enumeration {
  type ForeignKeyModifyAction = Value
  val SetNull, SetDefault, Restrict, NoAction, Cascade = Value

  def generateString(a: Value, t: DeleteOrUpdate.Value): String =
    a match {
      case ForeignKeyModifyAction.SetNull =>
        s"\nON ${DeleteOrUpdate.generateString(t)} SET NULL"
      case ForeignKeyModifyAction.SetDefault =>
        s"\nON ${DeleteOrUpdate.generateString(t)} SET DEFAULT"
      case ForeignKeyModifyAction.Restrict =>
        s"\nON ${DeleteOrUpdate.generateString(t)} RESTRICT"
      case ForeignKeyModifyAction.NoAction =>
        s"\nON ${DeleteOrUpdate.generateString(t)} NO ACTION"
      case ForeignKeyModifyAction.Cascade =>
        s"\nON ${DeleteOrUpdate.generateString(t)} CASCADE"
    }
}

object Conjunction extends Enumeration {
  type Conjunction = Value
  val AND, OR = Value

  def generateString(a: Value): String =
    a match {
      case Conjunction.AND => "AND"
      case Conjunction.OR  => "OR"
    }
}
