# how did i set this up:

# i configured my dlt mcp server 
# i installed the pyenv 3.11.9 because it said in the requirements to use python 3.11+
# i set the local python version as 3.11.9 with pyenv local 3.11.9 in the dlt-workshop directory for all the dlt activities which created the .python-version file 
# i created this folder here taxi-pipeline 
# i initialized it as uv managed python project with uv init 
# i installed uv add "dlt[workspace]" see e.g. https://dlthub.com/context/source/open-library 
# i executed uv run dlt init dlthub:taxi_pipeline duckdb see e.g. https://dlthub.com/context/source/open-library
# it doesnt have the source configuration scuffolding so i figured that out myself using the infos: 

Build a REST API source for NYC taxi data.

API details:
- Base URL: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
- Data format: Paginated JSON (1,000 records per page)
- Pagination: Stop when an empty page is returned

Place the code in taxi_pipeline.py and name the pipeline taxi_pipeline.
Use @dlt rest api as a tutorial.

# then i iterate testing uv run taxi_pipeline_pipeline.py
# capture execution logs with: uv run taxi_pipeline_pipeline.py 2>&1 | tee execution_logs.txt

# tested dlt dashboard: uv run dlt pipeline taxi_pipeline show 

# exploring the data with marimo and ibis https://dlthub.com/docs/general-usage/dataset-access/marimo

# promt: https://dlthub.com/docs/general-usage/dataset-access/marimo : using the above reference, create plots that will answer the following questions: Question 1. What is the start date and end date of the dataset?, Question 2. What proportion of trips are paid with credit card?, Question 3. What is the total amount of money generated in tips?

# -> did not do anything reasonable, at least not with my llm model here 

# answered homework questions: 

-- what is the start date and end date of the data set? 

select
  min (trip_pickup_date_time) as earliest_pudt,
  min (trip_dropoff_date_time) as earliest_dodt,
  max (trip_pickup_date_time) as latest_pudt,
  max (trip_dropoff_date_time) as latest_dodt
from taxi_pipeline.taxi_pipeline_dataset.taxi_trips


-- Question 2. What proportion of trips are paid with credit card? (1 point)

select 
  distinct payment_type
from taxi_pipeline.taxi_pipeline_dataset.taxi_trips;

select 
-- case when payment_type = 'Credit' then 1 else 0 end as count_credit_card_trips
  sum(case when payment_type = 'Credit' then 1 else 0 end) / count(*) as credit_card_percentage
from taxi_pipeline.taxi_pipeline_dataset.taxi_trips;

-- Question 3. What is the total amount of money generated in tips? (1 point)
select
  sum(tip_amt) as total_tip_amt
from taxi_pipeline.taxi_pipeline_dataset.taxi_trips;






