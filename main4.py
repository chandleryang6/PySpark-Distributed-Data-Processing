#!/usr/bin/env python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, row_number
from pyspark.sql.window import Window

def main():
    input_file = sys.argv[1] #Reads in first argument of command line: Path to input data
    map_file = sys.argv[2] #Reads in second argument of command line: Path to the map data file
    output_dir = sys.argv[3] #Reads in third argument of command line: Path to output directory

    spark = SparkSession.builder.appName("Task4").getOrCreate() #Initializes SparkSession
    trip_df = spark.read.csv(input_file, header = True, inferSchema = True) #Reads input file into dataframe
    map_df = spark.read.csv(map_file, header = True, inferSchema = True) #Reads map file into dataframe
    filtered_trip_df = trip_df.filter(col("Trip_distance") > 2) #Filters trip distances that are > 2 miles 
    pickup_hour = filtered_trip_df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) #Gets hour of pickup 
    brooklyn_df = map_df.filter(col("Borough") == "Brooklyn") #Filters for Brooklyn zones

    #Joins trip data with map data on LocationID
    joined_df = pickup_hour.join(
        brooklyn_df, pickup_hour["PULocationID"] == map_df["LocationID"]
    )

    #Groups by hour and zone to count pickups
    hourly_pickups = (
        joined_df
        .groupBy("pickup_hour", "Zone") #Groups df by pickup_hour and Zone columns
        .agg(count("*").alias("total_pickups"))  #Counts total pickups for each hour and zone
    )

    #Defines a window specification: partition by hour, order by total pickups (desc) and Zone (asc)
    window_spec = Window.partitionBy("pickup_hour").orderBy(col("total_pickups").desc(), col("Zone").asc()) 

    #Finds zone with largest number of pickups for each hour
    largest_num_pickup = (
        hourly_pickups
        .withColumn("rank", row_number().over(window_spec)) #Adds ranking column
        .filter(col("rank") == 1) #Filters only top-ranked zone for each hour
        .select("pickup_hour", "Zone") #Selects relevant columns
    )

    largest_num_pickup.write.mode("overwrite").csv(output_dir, header = False) #Saves result to output directory

if __name__ == "__main__":
    main()