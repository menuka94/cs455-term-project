package tickets

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import util.DataFields
import util.CodesList
import java.util

object ViolationTypeByYear {
  val YEAR_LIST = List("2013", "2014", "2015", "2016", "2017")
  val CODES_LIST: util.List[String] = CodesList.CODES_LIST

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage ViolationTypeByYear <parking-tickets-file-path> <output-file>")
      System.exit(0)
    }

    val PARKING_TICKETS_FILE_PATH = args(0)
    val OUTPUT_FILE = args(1)

    val spark = SparkSession.builder.appName("ViolationTypeByYear").getOrCreate()
    val sc = SparkContext.getOrCreate()
    import spark.implicits._

    val parkingData = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(PARKING_TICKETS_FILE_PATH/*"hdfs://topeka:4056/cs455/park/*.csv"*/*/)

    val trimmedData = parkingData.select(DataFields.VIOLATION_CODE, DataFields.ISSUE_DATE)

    val yearlyData = trimmedData.withColumn("Year",$"Issue Date".substr(7,4)).select("Year", "Violation Code")

    val filteredYearlyData = yearlyData.filter($"Year" isin YEAR_LIST).filter($"Violation Code" isin CODES_LIST)

    val output = filteredYearlyData.groupBy("Year", "Violation Code").count.orderBy("Year", "Violation Code")

    val outputRDD = output.toJavaRDD

    outputRDD.saveAsTextFile(OUTPUT_FILE)


    //output.collect()

    //do an orderBy?
    //https://stackoverflow.com/questions/37039943/spark-scala-how-to-transform-a-column-in-a-df
    //https://stackoverflow.com/questions/33393815/count-instances-of-combination-of-columns-in-spark-dataframe-using-scala
    //https://alvinalexander.com/scala/how-to-use-scala-match-expression-like-switch-case-statement/
    //https://backtobazics.com/big-data/spark/apache-spark-groupby-example/
  }
}