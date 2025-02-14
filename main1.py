#!/usr/bin/env python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

def main():
    input_file = sys.argv[1] #Reads in first argument of command line: Path to input data
    output_dir = sys.argv[2] #Reads in second argument of command line: Path to output directory

    spark = SparkSession.builder.appName("Task1").getOrCreate() #Initializes SparkSession
    df = spark.read.csv(input_file, header = True, inferSchema = True) #Reads CSV data into dataframe
    filtered_df = df.filter(col("trip_distance") > 2) #Filters trip distances that are > 2 miles 

    #Groups by PULocationID and calculates total distance 
    result_df = (
        filtered_df
        .groupBy("PULocationID") #Groups df by PULocationID column
        .sum("trip_distance") #Calculates sum of trip_distance for each group
        .withColumnRenamed("sum(trip_distance)", "total_distance") #Renames resulting column 
        .withColumn("total_distance", round(col("total_distance"), 2)) #Rounds total_distance to 2 decimal places
    )

    sorted_df = result_df.orderBy("PULocationID") #Sorts by ascending order of PULocationID
    sorted_df.write.mode("overwrite").csv(output_dir, header = False) #Saves result to output directory

if __name__ == "__main__":
    main()