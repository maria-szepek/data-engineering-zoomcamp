# How to build: 

* export GEMINI_API_KEY="my-key"
* docker compose up -d 
* terraform: terraform apply to create bucket and dataset
* kestra ui: create flows from ./flows/* and create GCP_CREDS kv pair
* kestra ui: execute for both green and yellow with backfill 2019-01-01 00:00:00 to end time stamp 
* kestra ui: for extract file metrics, comment purge task 
* terraform: terraform destroy to remove all used resources

# Quiz: 

Quiz Questions: Complete the quiz shown below. It's a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra, and ETL pipelines.

    1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?
    • 128.3 MiB
    • 134.5 MiB
    • 364.7 MiB
    • 692.6 MiB

-> With kestra ui output info 

    2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?
    • {{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
    • green_tripdata_2020-04.csv
    • green_tripdata_04_2020.csv
    • green_tripdata_2020.csv

-> with zoomcamp.09_gcp_taxi_scheduled.yaml 

    3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
    • 13,537.299
    • 24,648,499
    • 18,324,219
    • 29,430,127

With query editor in https://console.cloud.google.com/bigquery...
```
select count(1)
from `demo_dataset.yellow_tripdata`
where filename like 'yellow_tripdata_2020%'
```

    4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
    • 5,327,301
    • 936,199
    • 1,734,051
    • 1,342,034

With query editor in https://console.cloud.google.com/bigquery...
```
select count(1)
from `demo_dataset.green_tripdata`
where filename like 'green_tripdata_2020%'
```

    5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
    • 1,428,092
    • 706,911
    • 1,925,152
    • 2,561,031

With query editor in https://console.cloud.google.com/bigquery...
```
select count(1)
from `demo_dataset.yellow_tripdata`
where filename like 'yellow_tripdata_2021-03%'
```

    6. How would you configure the timezone to New York in a Schedule trigger?
    • Add a timezone property set to EST in the Schedule trigger configuration
    • Add a timezone property set to America/New_York in the Schedule trigger configuration
    • Add a timezone property set to UTC-5 in the Schedule trigger configuration
    • Add a location property set to New_York in the Schedule trigger configuration

With https://kestra.io/docs/workflow-components/triggers/schedule-trigger: 
```
triggers:
  - id: daily
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "@daily"
    timezone: America/New_York
```
