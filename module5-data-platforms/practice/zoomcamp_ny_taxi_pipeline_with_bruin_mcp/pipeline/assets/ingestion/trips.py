"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: integer
    description: A code indicating the TLCB-licensed taxi provider
  - name: tpep_pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged
  - name: tpep_dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged
  - name: passenger_count
    type: float
    description: The number of passengers in the vehicle
  - name: trip_distance
    type: float
    description: The elapsed trip distance in miles reported by the taximeter
  - name: ratecode_id
    type: float
    description: The final rate code in effect at the end of the trip
  - name: store_and_fwd_flag
    type: string
    description: Flag indicating whether the trip data was sent immediately to the vendor
  - name: pu_location_id
    type: integer
    description: Taxi zone ID for pickup location
  - name: do_location_id
    type: integer
    description: Taxi zone ID for dropoff location
  - name: payment_type
    type: integer
    description: A numeric value that corresponds to how the passenger paid for the trip
  - name: fare_amount
    type: float
    description: The time-and-distance fare calculated by the meter
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: MTA tax
  - name: tip_amount
    type: float
    description: Tip amount - automatically populated for credit card tipping; zero for cash tips
  - name: tolls_amount
    type: float
    description: Total amount of all tolls paid in trip
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge
  - name: total_amount
    type: float
    description: The total amount charged to the passenger
  - name: congestion_surcharge
    type: float
    description: Total amount collected in congestion surcharges for trip
  - name: airport_fee
    type: float
    description: Airport fee
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)

@bruin"""

import os
import json
from datetime import datetime, timedelta
import pandas as pd
import requests
import pyarrow.parquet as pq
from io import BytesIO


def materialize():
    """
    Fetch NYC Taxi Trip data from TLC public endpoint.
    
    Uses BRUIN_START_DATE/END_DATE to determine date range.
    Uses BRUIN_VARS to get taxi_types variable.
    """
    # Get environment variables
    start_date_str = os.environ.get('BRUIN_START_DATE', '2022-01-01')
    end_date_str = os.environ.get('BRUIN_END_DATE', '2022-01-31')
    bruin_vars = os.environ.get('BRUIN_VARS', '{}')
    
    # Parse variables
    try:
        vars_dict = json.loads(bruin_vars)
        taxi_types = vars_dict.get('taxi_types', ['yellow', 'green'])
    except:
        taxi_types = ['yellow', 'green']
    
    # Parse dates
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    # Generate month/year combinations
    current = start_date
    date_months = []
    while current <= end_date:
        year = current.year
        month = current.month
        date_months.append((year, month))
        # Move to next month
        if month == 12:
            current = datetime(year + 1, 1, 1)
        else:
            current = datetime(year, month + 1, 1)
    
    # Fetch data for each taxi type and month
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
    all_dfs = []
    
    for taxi_type in taxi_types:
        for year, month in date_months:
            # Format month as zero-padded string
            month_str = f'{month:02d}'
            filename = f'{taxi_type}_tripdata_{year}-{month_str}.parquet'
            url = base_url + filename
            
            try:
                # Fetch parquet file
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    # Read parquet from bytes
                    parquet_file = pq.read_table(BytesIO(response.content))
                    df = parquet_file.to_pandas()
                    # Add taxi_type column
                    df['taxi_type'] = taxi_type
                    all_dfs.append(df)
                    print(f"✓ Fetched {filename}: {len(df)} rows")
                else:
                    print(f"✗ Failed to fetch {filename}: HTTP {response.status_code}")
            except Exception as e:
                print(f"✗ Error fetching {filename}: {str(e)}")
    
    if not all_dfs:
        raise ValueError("No data was successfully fetched from TLC endpoint")
    
    # Concatenate all dataframes
    result_df = pd.concat(all_dfs, ignore_index=True)
    
    print(f"✓ Total rows extracted: {len(result_df)}")
    
    return result_df



