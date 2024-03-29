package tickets

import org.apache.spark.{SparkConf, SparkContext}
import util.{CountyName, IntDataFields}

object HighestMonthByCounty {
  val COUNTY_LIST = List(CountyName.BRONX_COUNTY, CountyName.NEW_YORK_COUNTY, CountyName.KINGS_COUNTY, CountyName.QUEENS_COUNTY, CountyName.RICHMOND_COUNTY)
  val monthMap = Map(1 -> "Jan", 2 -> "Feb", 3 -> "Mar", 4 -> "Apr", 5 -> "May", 6 -> "Jun", 7 -> "Jul", 8 -> "Aug", 9 -> "Sep", 10 -> "Oct", 11 -> "Nov", 12 -> "Dec")
  val SEPARATOR = ":"

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage HighestHourByCounty <parking-tickets-file-path> <output-file>")
      System.exit(0)
    }

    val PARKING_TICKETS_FILE_PATH = args(0)
    val OUTPUT_FILE = args(1)
    val NUMBER_OF_PARTITIONS = 100

    val conf = new SparkConf().setAppName("HighestMonthByCounty")
    val sc = new SparkContext(conf)

    val sb = new StringBuilder("NYC Parking Tickets\n")

    //Getting all of the incident data that is needed for analysis
    var parking_tickets = sc.textFile(PARKING_TICKETS_FILE_PATH, NUMBER_OF_PARTITIONS)

    // remove header row from the rdd
    parking_tickets = parking_tickets.filter(row => !row.startsWith("Summons"))

    val total_count = parking_tickets.count()
    sb.append("Total Records : " + total_count + "\n\n")

    // do some preprocessing on violation time and county
    var processedRdd = parking_tickets.map(value => {

      val data = value.split(",")
      try {
        // check for null, empty fields and Violation Time length to be exactly 5
        if (data.length >= 21 && !data(IntDataFields.ISSUE_DATE).equals(null) && !data(IntDataFields.ISSUE_DATE).isEmpty
          && data(IntDataFields.ISSUE_DATE).length == 10 && data(IntDataFields.VIOLATION_TIME).length >= 2
          && data(IntDataFields.VIOLATION_TIME).substring(0, 2).toInt < 12
          && !data(IntDataFields.VIOLATION_COUNTY).equals(null) && !data(IntDataFields.VIOLATION_COUNTY).isEmpty) {
          val county = get_county(data(IntDataFields.VIOLATION_COUNTY))
          val month = get_month(data(IntDataFields.ISSUE_DATE))

          county + SEPARATOR + month
        }
        else // all invalid entries for Violation Time and Violation County will return empty strings
        {
          ("")
        }
      } catch {
        case e: NumberFormatException => {
          println("Non-integer found")
          ("")
        }
      }
    })

    //remove empty values from the RDD
    processedRdd = processedRdd.filter(value => !value.isEmpty)

    val new_total = processedRdd.count()
    sb.append("Records after preprocessing : " + new_total + "\n\n")

    print(sb.toString())

    //processedRdd.foreach( value => println(value))

    // create a pair RDD; key -> county:hour, value -> 1
    val countyHourPairRDD = processedRdd.map(value => Tuple2(value, 1))

    val countyHourCountRDD = countyHourPairRDD.reduceByKey(_ + _).persist()

    var topList = List[String]()

    for (county_name <- COUNTY_LIST) {
      val countyData = countyHourCountRDD.filter(x => x._1.startsWith(county_name))

      // sort per county key,value pairs by value and pick the key with the highest value
      var topEntry = countyData.sortBy(pair => pair._2, ascending = false, numPartitions = NUMBER_OF_PARTITIONS).keys.take(1)
      topList = topEntry.mkString :: topList
    }

    // convert list to a RDD and save it
    val topRDD = sc.parallelize(topList)

    val mappedRDD = topRDD.map(entry => Tuple2(entry.split(SEPARATOR)(0), monthMap(entry.split(SEPARATOR)(1).toInt)))

    mappedRDD.saveAsTextFile(OUTPUT_FILE)

  }

  /**
   * Extract month from date field
   *
   * @param str date with the format MM/DD/YYYY
   * @return month as an integer
   */
  def get_month(str: String): Int = {
    str.substring(0, 2).toInt
  }

  /**
   * This method creates standard county codes if they are not following the standard
   *
   * @param str Violation County in the dataset
   * @return Standard County Code
   */
  def get_county(str: String): String = {

    if (COUNTY_LIST.contains(str)) {
      str
    }
    else if (str == "RICH" || str == "RC") {
      CountyName.RICHMOND_COUNTY
    }
    else if (str == "QUEEN") {
      CountyName.QUEENS_COUNTY
    }
    else if (str == "BRONX") {
      CountyName.BRONX_COUNTY
    }
    else if (str == "KINGS" || str == "KI") {
      CountyName.KINGS_COUNTY
    }
    else {
      CountyName.NEW_YORK_COUNTY
    }
  }
}
