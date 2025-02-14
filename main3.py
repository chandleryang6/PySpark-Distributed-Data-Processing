#!/usr/bin/env python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, count, avg

def main():
    input_file = sys.argv[1] #Reads in first argument of command line: Path to input data
    output_dir = sys.argv[2] #Reads in second argument of command line: Path to output directory

    spark = SparkSession.builder.appName("Task3").getOrCreate() #Initializes SparkSession
    df = spark.read.csv(input_file, header = True, inferSchema = True) #Reads CSV data into dataframe
    filtered_df = df.filter(col("trip_distance") > 2) #Filters trip distances that are > 2 miles 

    #Gets date and day of the week from pickup datetime
    date_df = (
        filtered_df
        .withColumn("pickup_date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd")) #Gets date 
        .withColumn("day_of_week", date_format(col("tpep_pickup_datetime"), "EEEE")) #Gets day of the week 
    )

    #Groups by date and day of the week to compute total pickups per day
    daily_pickups = (
        date_df
        .groupBy("pickup_date", "day_of_week") #Groups df by pickup_date and day_of_week columns
        .count() #Counts number of pickups for each group 
        .withColumnRenamed("count", "daily_pickups") #Renames resulting column 
    )

    #Groups by day of the week to compute average pickups
    daily_avg_pickups = (
        daily_pickups
        .groupBy("day_of_week") #Groups df by day_of_week column
        .agg(avg("daily_pickups").alias("avg_pickups")) #Computes average of daily_pickups for each day of the week and renames it 
    )

    #Sorts by average pickups and day order 
    sorted_average = daily_avg_pickups.orderBy(
        col("avg_pickups").desc(), #Sort days by average pickups
        col("day_of_week") #Sorts alphabetically for tie
    )

    top_3_days = sorted_average.limit(3).select("day_of_week") #Selects top 3 days
    top_3_days.write.mode("overwrite").csv(output_dir, header = False) #Saves result to output directory

if __name__ == "__main__":
    main()