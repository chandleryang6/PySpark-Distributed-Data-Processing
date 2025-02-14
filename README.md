# **NYC Yellow Taxi Data Analysis with PySpark**

## **Project Overview**
This project implements **PySpark-based data analysis** on **NYC Yellow Taxi trip data**. The goal is to efficiently process large-scale structured data using **PySpark DataFrame and RDD API**, without using SQL queries or external libraries like Pandas. The analysis includes:

- Computing trip distances by pickup location.
- Identifying most active locations.
- Analyzing daily pickup trends.
- Finding peak hours in Brooklyn.

---

## **Installation & Setup**
### **Prerequisites**
- Python 3.10 or higher
- PySpark (`pip install pyspark`)
- Hadoop (optional for local testing)

### **Installing PySpark**
Run the following command to install PySpark:

```bash
pip install pyspark
```

### **Dataset**
Due to GitHub's file size limit, the dataset is hosted on Google Drive.  
[Download NYC Yellow Taxi Dataset](https://drive.google.com/file/d/1D0MUXh6SPT7sPLl-xoNm_zyPEvujmOAH/view)  
After downloading, place data.csv in the project directory before running the scripts.

### Data Description
The dataset contains NYC taxi trip records with 19 columns, including:
- tpep_pickup_datetime – Pickup timestamp
- tpep_dropoff_datetime – Dropoff timestamp
- Passenger_count – Number of passengers
- Trip_distance – Distance traveled (miles)
- PULocationID – Pickup location (TLC zone)
- DOLocationID – Dropoff location (TLC zone)


```php
Project Directory
 ├── main1.py   # Computes total trip distance by PULocationID
 ├── main2.py   # Identifies the top 10 most active location IDs
 ├── main3.py   # Lists the top 3 days with the highest average pickups
 ├── main4.py   # Determines peak pickup zones in Brooklyn by hour
 ├── data.csv   # NYC Yellow Taxi dataset (too large for GitHub)
 ├── map.csv    # Location ID to borough and zone mapping file
 ├── README.md  # Project documentation
```

## **Installation & Setup**
Each script reads the dataset from the command line and writes results to a specified directory.  

### Steps
1. Clone the Repository
```bash
git clone https://github.com/chandleryang6/PySpark-Distributed-Data-Processing.git
cd PySpark-Distributed-Data-Processing
```

2. Run the Scripts:    
**Task 1:** Compute Total Trip Distance by PULocationID  
 - Filters trips with distances greater than 2 miles.
 - Aggregates total trip distance per PULocationID.
 - Saves results in a CSV file.
Run Command
```bash
python main1.py data.csv output1
```

**Task 2:** Identify Top 10 Most Active Location IDs
 - Defines activity as the total number of pickups and drop-offs.
 - Computes top 10 most active LocationIDs.
 - Resolves ties by choosing the lower LocationID.
 - Outputs sorted list in descending order.
Run Command
```bash
python main2.py data.csv output2
```

**Task 3:** Top 3 Days with Highest Average Pickups
 - Groups by day of the week.
 - Computes average daily pickups for each day.
 - Selects top 3 days with the highest values.
 - In case of a tie, sorts Monday → Sunday.
Run Command
```bash
python main3.py data.csv output3
```

**Task 4:** Most Popular Pickup Zones in Brooklyn by Hour
 - Uses an external map file (map.csv) to map LocationID to Borough & Zone.
 - For each hour (00-23), identifies the Brooklyn zone with the most pickups.
 - Resolves ties by alphabetical order of zone names.
Run Command
```bash
python main4.py data.csv map.csv output4
```

### Technologies Used
- Python 
- PySpark (Apache Spark) – Distributed data processing
- Hadoop Distributed File System (HDFS) – Handling large-scale data (conceptually)
- Linux Command Line – Execution and file handling
