package vehicles

import org.apache.spark.{SparkConf, SparkContext}
import util.{CountyName, IntDataFields}

/**
 * Find the most recorded vehicle make for each county
 */
object VehicleMakeByCounty {

  val COUNTY_LIST = List(CountyName.BRONX_COUNTY, CountyName.NEW_YORK_COUNTY, CountyName.KINGS_COUNTY,
    CountyName.QUEENS_COUNTY, CountyName.RICHMOND_COUNTY)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: VehicleMakeByCounty <parking-tickets-file-path> <output-file>")
      System.exit(0)
    }

    val PARKING_TICKETS_FILE_PATH = args(0)
    val OUTPUT_FILE = args(1)
    val NUMBER_OF_PARTITIONS = 100

    val conf = new SparkConf().setAppName("VehicleMakeByCounty")
    val sc = new SparkContext(conf)

    //    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    //    val sc = spark.sparkContext

    val sb = new StringBuilder("NYC Parking Tickets\n")

    //Getting all of the incident data that is needed for analysis
    var parking_tickets = sc.textFile(PARKING_TICKETS_FILE_PATH, NUMBER_OF_PARTITIONS)

    // remove header row from the rdd
    parking_tickets = parking_tickets.filter(row => !row.startsWith("Summons"))

    // do some preprocessing on violation time and county
    var processedRdd = parking_tickets.map(value => {

      val data = value.split(",")

      if (data.length >= 21 && !data(IntDataFields.VEHICLE_MAKE).equals(null) && !data(IntDataFields.VEHICLE_MAKE).isEmpty
        && !data(IntDataFields.VIOLATION_COUNTY).equals(null) && !data(IntDataFields.VIOLATION_COUNTY).isEmpty) {
        val county = get_county(data(IntDataFields.VIOLATION_COUNTY))
        val make = data(IntDataFields.VEHICLE_MAKE)

        (county, make)
      } else { // all invalid entries for Violation Time and Violation County will return empty strings
        ()
      }
    })

    processedRdd = processedRdd.filter(value => value != null && value.toString.length > 0)

    processedRdd.collect().foreach(println)



    //    // create a pair RDD; key -> county:make, value -> 1
    //    val countyMakePairRDD = processedRdd.map(value => Tuple2(value, 1))
    //
    //    val countyMakeCountRDD = countyMakePairRDD.reduceByKey(_ + _).persist()
    //
    //    var topList = List[String]()
    //
    //    for (county_name <- COUNTY_LIST) {
    //      val countyData = countyMakeCountRDD.filter(x => x._1.startsWith(county_name))
    //
    //      // sort per county key,value pairs by value and pick the key with the highest value
    //      var topEntry = countyData.sortBy(pair => pair._2, ascending = false, numPartitions = NUMBER_OF_PARTITIONS).keys.take(1)
    //      topList = topEntry.mkString :: topList
    //    }
    //
    //    // convert list to a RDD and save it
    //    val topRDD = sc.parallelize(topList)
    //    topRDD.saveAsTextFile(OUTPUT_FILE)
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
    else if (str == "RICH" || str == "RC" || CountyName.Full.RICHMOND_COUNTY.equalsIgnoreCase(str)) {
      CountyName.RICHMOND_COUNTY
    }
    else if (str == "QUEEN" || CountyName.Full.QUEENS_COUNTY.equalsIgnoreCase(str)) {
      CountyName.QUEENS_COUNTY
    }
    else if (str == "BX" || CountyName.Full.BRONX_COUNTY.equalsIgnoreCase(str)) {
      CountyName.BRONX_COUNTY
    }
    else if (str == "KINGS" || str == "KI" || CountyName.Full.KINGS_COUNTY.equalsIgnoreCase(str)) {
      CountyName.KINGS_COUNTY
    }
    else {
      CountyName.NEW_YORK_COUNTY
    }
  }
}
