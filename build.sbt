name := "cs455-term-project"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.5"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j
  "org.apache.logging.log4j" % "log4j" % "2.13.1" pomOnly()
)

