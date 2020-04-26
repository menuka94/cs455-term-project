#!/bin/bash

rm -rf target
sbt package
${SPARK_HOME}/bin/spark-submit \
  --class tickets.ViolationsOverMonths \
  --deploye-mode cluster \
  --supervise target/scala-2.11/cs455-term-project_2.11-1.0.jar /home/nyc/${1}.csv /home/output/violations-over-months/${1}-${2} yarn
