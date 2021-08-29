
//import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
//import org.apache.spark.sql.Encoders
//import org.apache.spark.sql.types.StructType
//import org.scalatest.{BeforeAndAfter, FunSuite}
//
//abstract class TestDFProfile extends FunSuite
//  with DataFrameSuiteBase
//  with SharedSparkContext
//  with BeforeAndAfter {
//
//  //  point to resources and define schemas
//  val currentDirectory = new java.io.File(".").getCanonicalPath
//  // Using case class to define schema "while reading the file", with use of encoders
//  case class ExpectedSchemaClass(columnName: Option[String], count: String, min: String, max: String, mean: String, stddev: String, numDups: Long, numNulls: Double)
//  val ExpectedSchema: StructType = Encoders.product[ExpectedSchemaClass].schema
//
//  test("Dataframe DQ profiler") {
//
//    val ipDF = spark.read
//      .format("csv")
//      .option("header", "true")
//      .load(s"$currentDirectory/src/test/resources/testDataSet.csv")
//
//    // Reading a CSV with schema specified by a Case Class
//    val expectedDF = spark.read.schema(ExpectedSchema)
//      .format("csv")
//      .option("header", "true")
//      .load(s"$currentDirectory/src/test/resources/testDataSet_expected.csv")
//
//    //    ipDF.runProfile.show(false)
//
//  }
//}
