package regime.helper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object RegimeDFHelper {

  def checkIfTargetValueIsLesser(
      sourceDf: DataFrame,
      targetDf: DataFrame
  )(implicit spark: SparkSession): Option[Any] = {
    val sourceViewName = "SOURCE_DF"
    val targetViewName = "TARGET_DF"

    // create temp views
    sourceDf.createOrReplaceTempView(sourceViewName)
    targetDf.createOrReplaceTempView(targetViewName)

    // first value from each view
    val sourceFirstValue = RegimeSqlHelper.generateGetFirstValue(
      sourceViewName,
      sourceDf.columns(0)
    )
    val targetFirstValue = RegimeSqlHelper.generateGetFirstValue(
      targetViewName,
      targetDf.columns(0)
    )

    val resRow = spark
      .sql(s"""SELECT if(($sourceFirstValue) > ($targetFirstValue),($targetFirstValue),NULL)""")
      .toDF()
      .first()

    Some(resRow.get(0)).filter(_ => resRow.isNullAt(0))
  }
}
