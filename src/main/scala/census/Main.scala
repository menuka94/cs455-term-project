package census

import util.DataFields

object Main {
  private final val log = org.apache.log4j.LogManager.getLogger(Main.getClass.getName)
  private final val LOG_PREFIX = "************************** "

  def main(args: Array[String]) = {
    // start Spark Context
    val sqlSparkSession = org.apache.spark.sql.SparkSession.builder()
      .appName("nyc-parking-tickets-sql")
      .getOrCreate()

    if (args.length < 1) {
      println("Invalid number of arguments")
      System.exit(0)
    }

    val inputPath = args(0)

    val df = sqlSparkSession.read.format("csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(inputPath)

    //    val df2 = df.select(DataFields.VEHICLE_MAKE)
    //    val collectedDf2 = df2.collect()
    //    for (value <- collectedDf2) {
    //      log.info(LOG_PREFIX + value)
    //    }

    //    val df3 = df.select(DataFields.LATITUDE, DataFields.LONGITUDE)
    //    val collectedDf3 = df3.collect()
    //    for (value <- collectedDf3) {
    //      log.info(LOG_PREFIX + value)
    //    }

    val df4 = df.select(DataFields.VIOLATION_COUNTY)
    val distinctCounties = df4.select(df(DataFields.VIOLATION_COUNTY)).distinct()
    val collectedCounties = distinctCounties.collect()
    log.info(LOG_PREFIX + "Distinct Counties")
    for (value <- collectedCounties) {
      log.info(LOG_PREFIX + value);
    }


    log.info(LOG_PREFIX + " Describing dataframe")
    log.info(df.describe())

    sqlSparkSession.stop()
  }
}
