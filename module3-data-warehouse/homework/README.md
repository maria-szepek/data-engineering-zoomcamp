# How to build: 

* terraform: terraform apply to create all used resources (bucket and dataset)
* Execute Python script: load_yellow_taxi_data.py https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/03-data-warehouse/load_yellow_taxi_data.py, with appropriate bucket and GCP credentials

# BQ query editor queries: 

```
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-484903.demo_dataset.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc-de-course-484903-terra-bucket/yellow_tripdata_2024-*.parquet']
);

-- Check yellow trip data
SELECT * FROM dtc-de-course-484903.demo_dataset.external_yellow_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dtc-de-course-484903.demo_dataset.yellow_tripdata_non_partitioned AS
SELECT * FROM dtc-de-course-484903.demo_dataset.external_yellow_tripdata;

-- Count the records for the 2024 Yellow Taxi Data
select count(1) from demo_dataset.yellow_tripdata_non_partitioned;
-- 20332093

-- query to count the distinct number of PULocationIDs for the entire dataset on both the tables
select count(distinct PULocationID) from demo_dataset.external_yellow_tripdata; -- This query will process 0 B when run.
select count(distinct PULocationID) from demo_dataset.yellow_tripdata_non_partitioned; -- This query will process 155.12 MB when run.

-- query to retrieve the PULocationID from the table (not the external table) in BigQuery.
select PULocationID from demo_dataset.yellow_tripdata_non_partitioned; -- This query will process 155.12 MB when run.
-- Now write a query to retrieve the PULocationID and DOLocationID on the same table
select PULocationID, DOLocationID from demo_dataset.yellow_tripdata_non_partitioned; -- This query will process 310.24 MB when run.

-- How many records have a fare_amount of 0?
select count (1) from demo_dataset.yellow_tripdata_non_partitioned
where fare_amount = 0; -- 8333

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE dtc-de-course-484903.demo_dataset.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM dtc-de-course-484903.demo_dataset.external_yellow_tripdata;

-- This query will process 310.24 MB when run.
SELECT count(distinct VendorID) 
FROM demo_dataset.yellow_tripdata_non_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- This query will process 26.84 MB when run.
SELECT count(distinct VendorID) 
FROM demo_dataset.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- demo_dataset.yellow_tripdata_non_partitioned
-- `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why? (not graded)
select count(*)
from demo_dataset.yellow_tripdata_non_partitioned; -- This query will process 0 B when run.
select count(*) 
from demo_dataset.yellow_tripdata_partitioned_clustered; -- This query will process 0 B when run.
```
