package regime.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object RegimeFn {

  def formatLongToDatetime(
      columnName: String,
      timestampFormat: String
  ): DataFrame => DataFrame =
    (df: DataFrame) =>
      df.withColumn(
        columnName,
        to_timestamp(col(columnName).cast("String"), timestampFormat)
      )

  def formatLongToDatetime(
      newColumnName: String,
      originalColumnName: String,
      timestampFormat: String
  ): DataFrame => DataFrame =
    (df: DataFrame) =>
      df.withColumn(
        newColumnName,
        to_timestamp(col(originalColumnName).cast("String"), timestampFormat)
      )

  def concatMultipleColumns(
      newColumnName: String,
      columnNames: Seq[String],
      conStr: String
  ): DataFrame => DataFrame =
    (df: DataFrame) => {
      val cc = columnNames
        .foldLeft(Seq[Column]()) { (seq, ele) =>
          seq :+ col(ele) :+ lit(conStr)
        }
        .dropRight(1)

      df.withColumn(newColumnName, concat(cc: _*))
    }
}
