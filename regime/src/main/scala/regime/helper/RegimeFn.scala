package regime.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RegimeFn {

  private val format1 = "yyyyMMddHHmmss"

  def format_long_to_datetime(
      df: DataFrame,
      originalColumnName: String,
      newColumnName: String,
      timestampFormat: String
  ): DataFrame =
    df.withColumn(
      newColumnName,
      to_timestamp(col(originalColumnName).cast("String"), format1)
    )
}
