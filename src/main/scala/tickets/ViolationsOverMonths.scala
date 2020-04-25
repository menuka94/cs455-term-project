package tickets

import org.apache.spark.sql.SparkSession
import util.CountyName

object ViolationsOverMonths {
  val COUNTY_LIST =  List(CountyName.BRONX_COUNTY, CountyName.NEW_YORK_COUNTY, CountyName.KINGS_COUNTY, CountyName.QUEENS_COUNTY, CountyName.RICHMOND_COUNTY)
  val MONTH_MAP = Map( 1 -> "Jan", 2 -> "Feb", 3 -> "Mar", 4 -> "Apr", 5 -> "May", 6 -> "Jun", 7 -> "Jul", 8 -> "Aug", 9 -> "Sep", 10 -> "Oct", 11 -> "Nov", 12 -> "Dec")
  val SEPARATOR = ":"
  val YEAR_LIST = List("2014", "2015", "2016", "2017")

  def main(args: Array[String]): Unit = {
    val PARKING_TICKETS_FILE_PATH = args(0)
    val OUTPUT_FILE = args(1)
    val COUNTY_CODE = args(2)
    val NUMBER_OF_PARTITIONS = 100

    if(!COUNTY_LIST.contains(COUNTY_CODE))
      {
        println("Invalid state code provided.")
        System.exit(0)
      }

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val sc = spark.sparkContext

    val sb = new StringBuilder("NYC Parking Tickets\n")

    //Getting all of the incident data that is needed for analysis
    var parking_tickets = sc.textFile(PARKING_TICKETS_FILE_PATH,NUMBER_OF_PARTITIONS)

    // remove header row from the rdd
    parking_tickets = parking_tickets.filter(row => !row.startsWith("Summons"))

    val total_count = parking_tickets.count()
    sb.append("Total Records : " + total_count + "\n\n")

    // do some preprocessing on violation time and county
    var processedRdd = parking_tickets.map(value => {

      val data = value.split(",")

      // check for null, empty fields and Violation Time length to be exactly 5
      if( !data(4).equals(null) &&  !data(4).isEmpty && data(4).length == 10 && !data(21).equals(null) && !data(21).isEmpty )
      {
        val county = get_county(data(21))
        val month = get_month(data(4))
        val year = get_year(data(4))

        county+SEPARATOR+year+SEPARATOR+month
      }
      else // all invalid entries for Violation Time and Violation County will return empty strings
      {
        ("")
      }
    })

    //remove empty values from the RDD
    processedRdd = processedRdd.filter(value => !value.isEmpty)

//   val new_total = processedRdd.count()
    //    sb.append("Records after preprocessing : " + new_total + "\n\n")

    print(sb.toString())

    // create a RDD with county code as the key
    val countyRDD = processedRdd.map(x => Tuple2(x.split(SEPARATOR)(0), x ))

    // filter all records with only requested county
    val targetCountyRDD = countyRDD.filter( y => y._1 == COUNTY_CODE)

    // generate a new RDD with (key, value) -> (year:month, 1)
    val yearMonthRDD = targetCountyRDD.map( u => Tuple2(u._2.split(SEPARATOR)(1) + SEPARATOR + u._2.split(SEPARATOR)(2), 1) )

    val yearMonthCountRDD = yearMonthRDD.reduceByKey(_+_).persist()

    for (year <- YEAR_LIST){
      val yearlyData = yearMonthCountRDD.filter(x => x._1.startsWith(year))

      // extract month from the key and map it to actual month name
      val monthlyData = yearlyData.map( x => Tuple2(MONTH_MAP(x._1.split(SEPARATOR)(1).toInt), x._2) )

      // sort per month key,value pairs by value and pick the key with the highest value
      val monthlyEntriesSorted = monthlyData.sortBy(pair => pair._2, ascending = false, numPartitions = NUMBER_OF_PARTITIONS)

      val new_path = OUTPUT_FILE + "_" + year + "_" + COUNTY_CODE
      monthlyEntriesSorted.saveAsTextFile(new_path)

    }
  }

  /**
   * Extract year from date field
   * @param str date with the format MM/DD/YYYY
   * @return year as an integer
   */
  def get_year(str: String) : Int = {
    str.substring(6).toInt
  }

  /**
   * Extract month from date field
   * @param str date with the format MM/DD/YYYY
   * @return month as an integer
   */
  def get_month(str: String) : Int = {
    str.substring(0, 2).toInt
  }

  /**
   * This method creates standard county codes if they are not following the standard
   * @param str Violation County in the dataset
   * @return Standard County Code
   */
  def get_county(str: String) : String = {

    if( COUNTY_LIST.contains(str) )
    {
      str
    }
    else if(str == "RICH" || str == "RC")
    {
      CountyName.RICHMOND_COUNTY
    }
    else if(str == "QUEEN")
    {
      CountyName.QUEENS_COUNTY
    }
    else if(str == "BRONX")
    {
      CountyName.BRONX_COUNTY
    }
    else if(str == "KINGS" || str == "KI")
    {
      CountyName.KINGS_COUNTY
    }
    else
    {
      CountyName.NEW_YORK_COUNTY
    }
  }
}
