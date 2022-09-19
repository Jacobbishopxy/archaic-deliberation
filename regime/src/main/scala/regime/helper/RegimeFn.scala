package regime.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

import regime.Global

object RegimeFn {

  def formatStringToDate(
      columnName: String,
      dateFormat: String
  ): DataFrame => DataFrame =
    df =>
      df.withColumn(
        columnName,
        to_date(col(columnName), dateFormat)
      )

  def formatLongToDatetime(
      columnName: String,
      timestampFormat: String
  ): DataFrame => DataFrame =
    df =>
      df.withColumn(
        columnName,
        to_timestamp(col(columnName).cast("string"), timestampFormat)
      )

  def formatLongToDatetime(
      newColumnName: String,
      originalColumnName: String,
      timestampFormat: String
  ): DataFrame => DataFrame =
    df =>
      df.withColumn(
        newColumnName,
        to_timestamp(col(originalColumnName).cast("string"), timestampFormat)
      )

  def formatDatetimeToLong(
      columnName: String,
      timestampFormat: String
  ): DataFrame => DataFrame =
    df =>
      df.withColumn(
        columnName,
        date_format(col(columnName), timestampFormat).cast("long")
      )

  def formatDatetimeToLong(
      newColumnName: String,
      columnName: String,
      timestampFormat: String
  ): DataFrame => DataFrame =
    df =>
      df.withColumn(
        newColumnName,
        date_format(col(columnName), timestampFormat).cast("long")
      )

  def concatMultipleColumns(
      newColumnName: String,
      columnNames: Seq[String],
      conStr: String
  ): DataFrame => DataFrame =
    df => {
      val cc = Global.listIntersperse(columnNames.toList.map(c => col(c)), lit(conStr))

      df.withColumn(newColumnName, concat(cc: _*))
    }

  def fillNullValue(
      columns: Seq[String],
      replacedBy: String
  ): DataFrame => DataFrame =
    df => df.na.fill(replacedBy, columns)

  def fillNullValue(
      columns: Seq[String],
      replacedBy: Boolean
  ): DataFrame => DataFrame =
    df => df.na.fill(replacedBy, columns)

  def fillNullValue(
      columns: Seq[String],
      replacedBy: Long
  ): DataFrame => DataFrame =
    df => df.na.fill(replacedBy, columns)

  def fillNullValue(
      columns: Seq[String],
      replacedBy: Double
  ): DataFrame => DataFrame =
    df => df.na.fill(replacedBy, columns)

  def dropNullRow(
      columns: Seq[String]
  ): DataFrame => DataFrame =
    df => df.na.drop(columns)

  def whenNotInThen[A](
      columnName: String,
      valueSet: Seq[A],
      defaultValue: A
  ): DataFrame => DataFrame =
    df =>
      df.withColumn(
        columnName,
        when(!col(columnName).isin(valueSet: _*), defaultValue).otherwise(lit(columnName))
      )
}
