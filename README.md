## CS455 - Introduction to Distributed Systems

### Term Project

* Authors
    * Laksheen Mendis
    * Menuka Warushavithana
    * Philip Kirner

### How to Run
* Compile and package into a JAR file 
    * `sbt package`
* Running on a Cluster
    * Use the scripts in `scripts` directory
    
    
### Source Files
* HighestHourByCounty.scala
    * Find the hour of day when the highest number of traffic violations were recorded in each county in New York City
* HighestMonthByCounty.scala
    * Find the month of year when the highest number of traffic violations were recorded in each county in New York City
* ViolationTypeByYear.scala
    * Find the highest number of recorded violations for each month in each county in New York City
* VehicleMakeByCounty.scala
    * Find the top 5 vehicle makes associated with traffic violations in each county
* ViolationTypeByYear.scala
    * Find the most recorded types of traffic violations occurred in New York City for each year in the dataset.
    
### Other Scripts
* census-data/census_county_level_processor.py
    * Extract the census data for counties in New York City from U.S. Census Data 
    
   