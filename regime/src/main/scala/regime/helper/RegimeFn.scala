package regime.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object RegimeFn {

  def formatLongToDatetime(
      columnName: String,
      timestampFormat: String
  ): DataFrame => DataFrame =
    df =>
      df.withColumn(
        columnName,
        to_timestamp(col(columnName).cast("String"), timestampFormat)
      )

  def formatLongToDatetime(
      newColumnName: String,
      originalColumnName: String,
      timestampFormat: String
  ): DataFrame => DataFrame =
    df =>
      df.withColumn(
        newColumnName,
        to_timestamp(col(originalColumnName).cast("String"), timestampFormat)
      )

  def concatMultipleColumns(
      newColumnName: String,
      columnNames: Seq[String],
      conStr: String
  ): DataFrame => DataFrame =
    df => {
      val cc = columnNames
        .foldLeft(Seq[Column]()) { (seq, ele) =>
          seq :+ col(ele) :+ lit(conStr)
        }
        .dropRight(1)

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
