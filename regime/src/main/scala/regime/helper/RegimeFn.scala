package regime.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RegimeFn {

  def formatLongToDatetime(
      originalColumnName: String,
      newColumnName: String,
      timestampFormat: String
  ): DataFrame => DataFrame = (df: DataFrame) =>
    df.withColumn(
      newColumnName,
      to_timestamp(col(originalColumnName).cast("String"), timestampFormat)
    )

  // def withCompoundColumnForPK()
}
