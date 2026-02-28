"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
materialization:
  type: table
  strategy: append
columns:
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup datetime"
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff datetime"
  - name: passenger_count
    type: integer
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance"
  - name: payment_type
    type: string
    description: "Payment type identifier"
  - name: fare_amount
    type: float
    description: "Fare amount"
  - name: total_amount
    type: float
    description: "Total paid amount"
  - name: taxi_type
    type: string
    description: "Taxi type (yellow/green/etc)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the row was extracted by this asset"

@bruin"""

from __future__ import annotations

import os
import json
import io
import calendar
from datetime import datetime, date
from typing import List

import pandas as pd
import requests


def _month_iter(start: date, end: date) -> List[date]:
    """Yield first-of-month dates between start and end (inclusive)."""
    months: List[date] = []
    cur = date(start.year, start.month, 1)
    while cur <= end:
        months.append(cur)
        year = cur.year + (cur.month // 12)
        month = (cur.month % 12) + 1
        cur = date(year, month, 1)
    return months


def _download_parquet_to_df(url: str) -> pd.DataFrame | None:
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            return None
        return pd.read_parquet(io.BytesIO(resp.content), engine="pyarrow")
    except Exception:
        return None


def materialize() -> pd.DataFrame:
    """Download monthly parquet files from the TLC public bucket and return a concatenated DataFrame.

    Environment inputs (provided by Bruin runtime):
    - `BRUIN_START_DATE` / `BRUIN_END_DATE` (YYYY-MM-DD)
    - `BRUIN_VARS` (JSON string) - expects `taxi_types` key with list of taxi types
    """
    start_s = os.environ.get("BRUIN_START_DATE")
    end_s = os.environ.get("BRUIN_END_DATE")
    vars_s = os.environ.get("BRUIN_VARS")

    if not start_s or not end_s:
        raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be set by the pipeline runtime")

    start = datetime.fromisoformat(start_s).date()
    end = datetime.fromisoformat(end_s).date()

    taxi_types = ["yellow"]
    if vars_s:
        try:
            parsed = json.loads(vars_s)
            taxi_types = parsed.get("taxi_types", taxi_types)
        except Exception:
            pass

    base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    dfs: List[pd.DataFrame] = []
    extracted_at = datetime.utcnow()

    for taxi in taxi_types:
        for m in _month_iter(start, end):
            year = m.year
            month = f"{m.month:02d}"
            filename = f"{taxi}_tripdata_{year}-{month}.parquet"
            url = f"{base}/{filename}"
            df = _download_parquet_to_df(url)
            if df is None or df.shape[0] == 0:
                continue
            # normalize common datetime column names if present
            if "tpep_pickup_datetime" in df.columns:
                df = df.rename(columns={"tpep_pickup_datetime": "pickup_datetime", "tpep_dropoff_datetime": "dropoff_datetime"})
            if "lpep_pickup_datetime" in df.columns:
                df = df.rename(columns={"lpep_pickup_datetime": "pickup_datetime", "lpep_dropoff_datetime": "dropoff_datetime"})

            df["taxi_type"] = taxi
            df["extracted_at"] = extracted_at
            dfs.append(df)

    if not dfs:
        # Return empty dataframe with expected columns
        cols = [
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "payment_type",
            "fare_amount",
            "total_amount",
            "taxi_type",
            "extracted_at",
        ]
        return pd.DataFrame(columns=cols)

    out = pd.concat(dfs, ignore_index=True, sort=False)

    # Ensure common columns exist and have reasonable types
    if "pickup_datetime" in out.columns:
        out["pickup_datetime"] = pd.to_datetime(out["pickup_datetime"], errors="coerce")
    if "dropoff_datetime" in out.columns:
        out["dropoff_datetime"] = pd.to_datetime(out["dropoff_datetime"], errors="coerce")
    if "passenger_count" in out.columns:
        out["passenger_count"] = pd.to_numeric(out["passenger_count"], errors="coerce").astype("Int64")
    for c in ("trip_distance", "fare_amount", "total_amount"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    return out



