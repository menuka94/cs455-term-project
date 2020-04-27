#!/bin/bash

rm -rf target
sbt package
${SPARK_HOME}/bin/spark-submit --master yarn \
  --deploy-mode cluster \
  --class tickets.HighestHourByCounty \
  --supervise target/scala-2.11/cs455-term-project_2.11-1.0.jar /nyc /outputs/
