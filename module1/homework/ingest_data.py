#!/usr/bin/env python
# coding: utf-8

import pandas as pd 
from sqlalchemy import create_engine

# db connection
pg_user = "root"
pg_pass = "root"
pg_host = "localhost"
pg_port = "5432"
pg_db = "ny_taxi"
engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

# table 1: Download the green taxi trips data for November 2025
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
target_table = "green_tripdata_2025-11"


df = pd.read_parquet(
    url, 
)
# the file has 1.2M chunking for write out not needed 
df.to_sql(
    name=target_table,
    con=engine,
    if_exists='replace',
)

# table 2: dataset with zones
url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
target_table = "taxi_zone_lookup"
# raw data has 13K chunking not needed 

df = pd.read_csv(
    url,
)

df.to_sql(
    name=target_table,
    con=engine,
    if_exists='replace'
)

print("Done.")

""" if __name__ == '__main__':
    run() """