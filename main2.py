#!/usr/bin/env python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def main():
    input_file = sys.argv[1] #Reads in first argument of command line: Path to input data
    output_dir = sys.argv[2] #Reads in second argument of command line: Path to output directory

    spark = SparkSession.builder.appName("Task2").getOrCreate() #Initializes SparkSession
    df = spark.read.csv(input_file, header = True, inferSchema = True) #Reads CSV data into dataframe
    filtered_df = df.filter(col("Trip_distance") > 2) #Filters trip distances that are > 2 miles 

    #Counts activeness for pickup 
    pickup_total = (
        filtered_df
        .groupBy("PULocationID") #Groups df by PULocationID column
        .count() #Counts number of rows for each PULocationID
        .withColumnRenamed("count", "pickup_count") #Renames resulting column 
        .withColumnRenamed("PULocationID", "LocationID") #Renames PULocationID column 
        .withColumn("dropoff_count", col("pickup_count") * 0) #Adds new column w/ default value of 0
    )

    #Counts activeness for drop off 
    dropoff_total = (
        filtered_df
        .groupBy("DOLocationID") #Groups df by DOLocationID column
        .count() #Counts number of rows for each DOLocationID
        .withColumnRenamed("count", "dropoff_count") #Renames resulting column 
        .withColumnRenamed("DOLocationID", "LocationID") #Renames DOLocationID column 
        .withColumn("pickup_count", col("dropoff_count") * 0) #Adds new column w/ default value of 0
    )

    #Combines pickup and drop off totals for each LocationID
    combined_total = (
        pickup_total
        .union(dropoff_total) #Combines pickup_total` and dropoff_total into a df
        .groupBy("LocationID") #Groups df by LocationID column
        .agg(
            _sum("pickup_count").alias("pickup_total"), #Sums pickup_total column for each LocationID
            _sum("dropoff_count").alias("dropoff_total"), #Sums dropoff_total column for each LocationID
        )  
    )

    #Computes activeness and sorts by descending activeness, ascending LocationID
    sorted_df = (
        combined_total
        .withColumn("activeness", col("pickup_total") + col("dropoff_total")) #Calculates total activeness as sum of pickups and drop offs
        .select("LocationID", "activeness") #Selects only LocationID and activeness columns 
        .orderBy(col("activeness").desc(), col("LocationID").asc()) # Sorts by activeness (descending) and LocationID (ascending for ties)
        .limit(10)  
    )

    sorted_df.select("LocationID").write.mode("overwrite").csv(output_dir, header = False) # Step 8: Saves the result to the output directory

if __name__ == "__main__":
    main()