package tickets

import org.apache.spark.sql.SparkSession
import util.CountyName

object HighestHourByCounty {

  val COUNTY_LIST =  List(CountyName.BRONX_COUNTY, CountyName.NEW_YORK_COUNTY, CountyName.KINGS_COUNTY, CountyName.QUEENS_COUNTY, CountyName.RICHMOND_COUNTY)

  def main(args: Array[String]): Unit = {
    val PARKING_TICKETS_FILE_PATH = args(0)
    val OUTPUT_FILE = args(1)
    val NUMBER_OF_PARTITIONS = 100

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    val sc = spark.sparkContext

    val sb = new StringBuilder("NYC Parking Tickets\n")

    //Getting all of the incident data that is needed for analysis
    var parking_tickets = sc.textFile(PARKING_TICKETS_FILE_PATH,NUMBER_OF_PARTITIONS)

    // remove header row from the rdd
    parking_tickets = parking_tickets.filter(row => !row.startsWith("Summons"))

    // do some preprocessing on violation time and county
    var processedRdd = parking_tickets.map(value => {

      val data = value.split(",")

      // check for null, empty fields and Violation Time length to be exactly 5
      if( data.length >= 21 && !data(19).equals(null) &&  !data(19).isEmpty && data(19).matches("^[0-9]+$") && data(19).length == 5 && data(19).substring(0,2).toInt < 12 && !data(21).equals(null) && !data(21).isEmpty )
      {
        val county = get_county(data(21))
        val hour = get_hour(data(19))

        county+":"+hour
      }
      else // all invalid entries for Violation Time and Violation County will return empty strings
      {
        ("")
      }
    })

    //remove empty values from the RDD
    processedRdd = processedRdd.filter(value => !value.isEmpty)

    val new_total = processedRdd.count()
    sb.append("Records after preprocessing : " + new_total + "\n\n")

    print(sb.toString())

    //processedRdd.foreach( value => println(value))

    // create a pair RDD; key -> county:hour, value -> 1
    val countyHourPairRDD = processedRdd.map( value => Tuple2(value,1) )

    val countyHourCountRDD = countyHourPairRDD.reduceByKey(_+_).persist()

    var topList = List[String]()

    for (county_name <- COUNTY_LIST){
      val countyData = countyHourCountRDD.filter(x => x._1.startsWith(county_name))

//      if(county_name == "K")
//        {
//          countyData.foreach(x => print("Count by hour is " + x + "\n"))
//        }

      // sort per county key,value pairs by value and pick the key with the highest value
      var topEntry = countyData.sortBy(pair => pair._2, ascending = false, numPartitions = NUMBER_OF_PARTITIONS).keys.take(1)
      topList = topEntry.mkString :: topList
    }

    // convert list to a RDD and save it
    val topRDD = sc.parallelize(topList)
    topRDD.saveAsTextFile(OUTPUT_FILE)

//    for(li <- topList)
//      {
//        print("Top entry : " + li + "\n")
//      }

  }

  /**
   * Convert the Violation Time to Violation Hour
   * Ex: 0841A --> 0800A
   * @param time Violation Time in the dataset
   * @return Violation Hour
   */
  def get_hour(time:String) : String = {

    val std_hr = time.substring(0,2) + "00" + time.substring(4)
    std_hr
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
