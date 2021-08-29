import org.apache.spark.sql.SparkSession

import com.amazon.deequ.profiles.{ColumnProfilerRunner, StandardColumnProfile, NumericColumnProfile}

object PrintSparkConfig1 extends App {
  val spark = SparkSession.builder()
    .master("local[10]")
    .appName("SparkByExample")
    .getOrCreate();

  println("First SparkContext:")
  println("APP Name :"+spark.sparkContext.appName);
  println("Deploy Mode :"+spark.sparkContext.deployMode);
  println("Master :"+spark.sparkContext.master);
  Thread.sleep(5000*100)

  val sparkSession2 = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample-test")
    .getOrCreate();


  println("Second SparkContext:")
  println("APP Name :"+sparkSession2.sparkContext.appName);
  println("Deploy Mode :"+sparkSession2.sparkContext.deployMode);
  println("Master :"+sparkSession2.sparkContext.master);
}