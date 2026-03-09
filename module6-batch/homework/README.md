# Homework

* data source: wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet

# Question 1: Install Spark and PySpark

* Install Spark
* Run PySpark
* Create a local spark session
* Execute spark.version.
* What's the output?

Solution:
* java: I am using java with sdk default java 21.0.10-tem.
* pyspark: I am using pyspark managed by uv with uv add pyspark
* executed: yellow_november_2025.ipynb

Answer: Spark version: 4.1.1

# Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

6MB
25MB
75MB
100MB

Solution: 

* ./download_data.sh yellow 2025
* use yellow_november_2025.ipynb
* check output: 

```
data/repartitioned/yellow/2025/11:
total 98M
-rw-r--r-- 1 maria.szepek maria.szepek 25M Mar  9 16:30 part-00000-278de156-3246-460f-a2eb-5bc4d1165f33-c000.snappy.parquet
-rw-r--r-- 1 maria.szepek maria.szepek 25M Mar  9 16:30 part-00001-278de156-3246-460f-a2eb-5bc4d1165f33-c000.snappy.parquet
-rw-r--r-- 1 maria.szepek maria.szepek 25M Mar  9 16:30 part-00002-278de156-3246-460f-a2eb-5bc4d1165f33-c000.snappy.parquet
-rw-r--r-- 1 maria.szepek maria.szepek 25M Mar  9 16:30 part-00003-278de156-3246-460f-a2eb-5bc4d1165f33-c000.snappy.parquet
-rw-r--r-- 1 maria.szepek maria.szepek   0 Mar  9 16:30 _SUCCESS
```

Answer: 25M

# Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

62,610
102,340
162,604
225,768

Solution: 
* use yellow_november_2025.ipynb

Answer: 162604

# Question 4: Longest trip
What is the length of the longest trip in the dataset in hours?

22.7
58.2
90.6
134.5

Solution: 
* use yellow_november_2025.ipynb

Answer: 90.6

# Question 5: User Interface
Spark's User Interface which shows the application's dashboard runs on which local port?

80
443
4040
8080

Answer: 4040

# Question 6: Least frequent pickup location zone
Load the zone lookup data into a temp view in Spark:

wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

Governor's Island/Ellis Island/Liberty Island
Arden Heights
Rikers Island
Jamaica Bay
If multiple answers are correct, select any

Solution: 
* use yellow_november_2025.ipynb

Answer: any from 
```
+---------------------------------------------+-----------+
|zone                                         |trips_count|
+---------------------------------------------+-----------+
|Governor's Island/Ellis Island/Liberty Island|1          |
|Eltingville/Annadale/Prince's Bay            |1          |
|Arden Heights                                |1          |
```
