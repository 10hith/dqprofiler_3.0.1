
import com.amazon.deequ.profiles.{ColumnProfilerRunner, NumericColumnProfile}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{arrays_zip, col, collect_list, lit}

// Data class to hold the histogram information from ColumnProfile results
case class HistogramRecord(column_name: String, value: String, num_occurrences: Double, ratio: Double)

/**
 * Object that contains all the helper methods required for profiling a dataframe
 */
object ProfileHelpers extends Utils{
  import spark.implicits._
  /**
   * Use pattern matching to unpack numerical attributes from class NumericColumnProfile
   * @param profile
   * @return
   */
  def unpackNumericProfile(profile: com.amazon.deequ.profiles.ColumnProfile) = {
    val op = profile.asInstanceOf[NumericColumnProfile] match {
      case profile => (
        profile.column
        ,profile.approximateNumDistinctValues
        ,profile.completeness
        ,profile.mean.getOrElse(-1.0)
        ,profile.maximum.getOrElse(-1.0)
        ,profile.minimum.getOrElse(-1.0)
        ,profile.sum.getOrElse(-1.0)
        ,profile.stdDev.getOrElse(-1.0)
      )
    }
    op
  }

  /**
   * Converts a Map of ColumnProfiles into a dataFrame using patter matching
   * @param numericProfiles
   * @return
   */
  def numericProfilesAsDataFrame(numericProfiles: Map[String,com.amazon.deequ.profiles.ColumnProfile] ): DataFrame = {
    numericProfiles.
      map {
        x => x match {
          case (_:String, profile) => unpackNumericProfile(profile)
        }
      }.
      toSeq.
      toDF("column_name", "num_distinct_values", "completeness", "mean", "maximum", "minimum", "sum", "stdDev").
      withColumn("column_type", lit("numeric"))
  }


  /**
   * unpack nonNumeric ColumnProfile
   * @param profile
   * @return
   */
  def unpackNonNumericProfile(profile: com.amazon.deequ.profiles.ColumnProfile) = {
    val op = profile match {
      case profile => (
        profile.column,
        profile.approximateNumDistinctValues,
        profile.completeness
      )
    }
    op
  }

  def nonNumericProfilesAsDataFrame(nonNumericProfiles: Map[String,com.amazon.deequ.profiles.ColumnProfile] ): DataFrame = {
    nonNumericProfiles.
      map {
        x => x match {
          case (_:String, profile) => unpackNonNumericProfile(profile)
        }
      }.
      toSeq.
      toDF("column_name", "num_distinct_values", "completeness").
      withColumn("column_type", lit("non_numeric"))
  }

  /**
   * Extracts Histogram from the ColumnProfiler, which holds distribution information only for columns with low cardinality
   * @param result: Result of the ColumnProfiler
   * @return: Histogram for categorical data returned as a DataFrame
   */
  def extractHistogramFromColumnProfileResult(result: com.amazon.deequ.profiles.ColumnProfiles): DataFrame = {
    var histogramRecordAcc = Seq[HistogramRecord]()
    val columns = result.profiles.keys

    columns map {
      columnName => result.profiles(columnName).histogram.foreach {
        _.values.foreach { case (key, entry) =>
          histogramRecordAcc = histogramRecordAcc:+HistogramRecord(result.profiles(columnName).column, key, entry.absolute, entry.ratio)
        }
      }
    }

    val rows = spark.sparkContext.parallelize(histogramRecordAcc)
    val histogramAsDf: DataFrame = spark.createDataFrame(rows)

    histogramAsDf.groupBy("column_name").agg(
      collect_list(col("value")).alias("value"),
      collect_list(col("num_occurrences")).alias("num_occurrences"),
      collect_list(col("ratio")).alias("ratio")
    ).withColumn("histogram", arrays_zip(
      col("value"),
      col("num_occurrences"),
      col("ratio")
    )).select(
      "column_name",
      "histogram"
    )
  }

  /**
   *
   * @param df - Input DataFrame to be profiled
   * @return DataFrame consisting of all column profiles
   */
  def runProfile(df: DataFrame): DataFrame = {

    // Run the Deequ profiler on the dataFrame
    val result: com.amazon.deequ.profiles.ColumnProfiles = ColumnProfilerRunner().
      onData(df).
      run()

    // Classify the numeric and nonNumeric profiles
    val numericProfiles = result.profiles filter { case (k,v) => v.isInstanceOf[com.amazon.deequ.profiles.NumericColumnProfile]}
    val nonNumericProfiles = result.profiles filter { case (k,v) => !v.isInstanceOf[com.amazon.deequ.profiles.NumericColumnProfile]}

    // Extract ColumnProfile values into DataFrame
    val nonNumeric_DF = nonNumericProfilesAsDataFrame(nonNumericProfiles)
    val numeric_DF = numericProfilesAsDataFrame(numericProfiles)

    // Extract ColumnProfile histogram values into DataFrame
    val histogram_DF = extractHistogramFromColumnProfileResult(result)

    // Join both numeric and nonNumeric datasets
    val profiled_DF =
      numeric_DF.
        join(nonNumeric_DF, Seq("column_name", "num_distinct_values", "completeness","column_type"), "outer").
        join(histogram_DF, Seq("column_name"), "outer")

    profiled_DF

  }

  /**
   * When the DataFrame has more than 500 columns, runProfile fails because
   * of the large DAG plan created. Below method breaks the DAG with use of df.cache
   * @param df
   * @return DataFrame consisting of all column profiles
   */
  def runProfileWideDf(df: DataFrame): DataFrame = {
    // group the columns
    val groupedCols = df.columns.grouped(500).toSeq.toArray.par

    // runProfile on each column group array, cache and reduce the resulting DataFrames
    val allProfiledDfs = groupedCols map { arr_cols =>
      runProfile(df.selectExpr(arr_cols: _*))
    } map {
      df => df.cache()
    } reduce {
      (df1, df2) => df1.union(df2)
    }

    allProfiledDfs
  }

}
