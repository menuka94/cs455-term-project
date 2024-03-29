#!/bin/bash

# Define a timestamp function
timestamp() {
  date +"%Y-%m-%d_%H-%M-%S"
}

rm -rf target
sbt package
${SPARK_HOME}/bin/spark-submit --master local --driver-memory 2g --executor-memory 2g \
  --class vehicles.VehicleMakeByCounty \
  target/scala-2.11/cs455-term-project_2.11-1.0.jar sample-data/ outputs/vehicle-make-by-county-$(timestamp)
