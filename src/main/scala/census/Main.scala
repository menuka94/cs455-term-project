package census

import util.DataFields

object Main {
  private final val logger = org.apache.log4j.LogManager.getLogger(Main.getClass.getName)
  private final val LOG_PREFIX = "**************************"

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

    val columns = Seq(DataFields.VEHICLE_MAKE, DataFields.REGISTRATION_STATE)
    val df2 = df.select()

    logger.info(LOG_PREFIX + " Describing dataframe")
    logger.info(df.describe())

    sqlSparkSession.stop()
  }
}
