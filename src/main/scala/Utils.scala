import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import java.time.LocalDateTime

import org.apache.spark.SparkContext

trait Utils {
  //  val spark: SparkSession = SparkSession.builder.getOrCreate()

  def printRunTime(step: String): Unit = {
    println(s"$step started at ${LocalDateTime.now()}")
  }

  def getArgValue (argsMap: Map[String, String] ) (key: String): String = {
    /**
     * Handles the arguments passed to main, looks up for the key and fetches the associated value
     * Usage...
     *      val argsMap = args.toMap
     *      // Handle the arguments, Throw exception when an input is not available
     *      val getValue = getArgValue(argsMap)(_)
     *      val dbName = getValue("dbName")
     */
    val noElementException: NoSuchElementException = new NoSuchElementException (s"Unable to find input argument for $key ")
    argsMap.getOrElse(s"$key", throw noElementException)
  }

  //  Method to refresh table and compute statistics
  def refreshTable(dbName: String, tblName: String): Unit =
  {
    spark.sql(s"REFRESH TABLE $dbName.$tblName")
    spark.sql(s"ANALYZE TABLE $dbName.$tblName COMPUTE STATISTICS")
  }

  def transformColumns(f: Column => Column)(cols: Column*)(appendString: String = "")(df: DataFrame): DataFrame = {
    // Sequentially apply a function "f" to input columns "cols", If append string provided, new columns are added to dataframe
    cols.foldLeft(df) {
      (df, x) => df.withColumn(s"$x$appendString".replace("date_", "days_since_"), f(x))
    }
  }

  //   Building local spark session
  lazy val spark = { SparkSession.builder()
    .appName("deUtils")
    //      .master("local")
//    .enableHiveSupport()
    .getOrCreate()
  }
  lazy val sc: SparkContext = spark.sparkContext


}