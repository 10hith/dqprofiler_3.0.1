

import org.apache.spark.sql.SparkSession

import ProfileHelpers.runProfileWideDf


object displaySparkSetup extends Utils{
  def main(args: Array[String] ): Unit = {

    val spark = { SparkSession.builder()
      .appName("deUtils")
      .master("local")
//      .enableHiveSupport()
      .getOrCreate()
    }

    val currentDirectory = new java.io.File(".").getCanonicalPath
    val ipDF = spark.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(s"$currentDirectory/src/test/resources/testDataSet_1.csv")


    spark.conf.getAll map (println(_))

    ipDF.show(200)
    ipDF.printSchema()

    val profiledDF = runProfileWideDf(ipDF)

    profiledDF.show(100)


  }
}