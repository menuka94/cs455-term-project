#!/bin/bash

#sbt package;
${SPARK_HOME}/bin/spark-submit --master local --driver-memory 2g --executor-memory 2g \
	--class census.Main \
	target/scala-2.11/cs455-term-project_2.11-1.0.jar dataset/2014.csv;
