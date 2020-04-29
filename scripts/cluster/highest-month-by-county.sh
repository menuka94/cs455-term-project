#!/bin/bash

# Define a timestamp function
timestamp() {
  date +"%Y-%m-%d_%H-%M-%S"
}

rm -rf target
sbt package
${SPARK_HOME}/bin/spark-submit --master yarn \
  --class tickets.HighestMonthByCounty \
  --deploy-mode cluster \
  --supervise target/scala-2.11/cs455-term-project_2.11-1.0.jar /nyc /outputs/highest-month-by-county-$(timestamp)
