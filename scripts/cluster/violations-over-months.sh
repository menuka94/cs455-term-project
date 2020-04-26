#!/bin/bash

rm -rf target
sbt package
${SPARK_HOME}/bin/spark-submit --master yarn \
  --class tickets.ViolationsOverMonths \
  --deploy-mode cluster \
  --supervise target/scala-2.11/cs455-term-project_2.11-1.0.jar /nyc/${1}.csv /outputs/violations-over-months-${1}-${2}
