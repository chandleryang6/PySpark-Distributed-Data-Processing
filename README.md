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

### **Tasks & Implementation**
Each script reads the dataset from the command line and writes results to a specified directory.  

1. Compute Total Trip Distance by PULocationID  
- Filters trips with distances greater than 2 miles.
- Aggregates total trip distance per PULocationID.
- Saves results in a CSV file.
Run Command
```bash
python main1.py data.csv output1
```

