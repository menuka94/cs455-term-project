package tickets

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import util.DataFields

object ViolationTypeByYear {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("ParkingTicket0").getOrCreate()
    val sc = SparkContext.getOrCreate()

    val parkingData = spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("hdfs://topeka:4056/cs455/park/*.csv")
    val trimmedData = parkingData.select(DataFields.VIOLATION_CODE, DataFields.ISSUE_DATE)

    val output = trimmedData.groupBy(DataFields.VIOLATION_CODE, get_year(DataFields.ISSUE_DATE)).count

  }

  def get_year(str: String): String = {
    str.substring(6)
  }
}