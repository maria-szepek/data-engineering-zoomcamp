/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table


custom_checks:
  - name: row_count_positive
    description: ensures the table is not empty
    query: |
      SELECT count(*) > 0 FROM staging.trips
    value: 1


@bruin */

-- Staging: normalize column names, deduplicate by composite key, join payment lookup

WITH raw AS (
  SELECT
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pu_location_id AS pickup_location_id,
    do_location_id AS dropoff_location_id,
    payment_type,
    fare_amount,
    total_amount,
    taxi_type,
    extracted_at
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
),

deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        COALESCE(CAST(pickup_location_id AS VARCHAR), ''),
        COALESCE(CAST(dropoff_location_id AS VARCHAR), ''),
        COALESCE(CAST(fare_amount AS VARCHAR), ''),
        COALESCE(CAST(total_amount AS VARCHAR), '')
      ORDER BY extracted_at DESC
    ) AS rn
  FROM raw
)

SELECT
  d.pickup_datetime,
  d.dropoff_datetime,
  d.passenger_count,
  d.trip_distance,
  d.pickup_location_id,
  d.dropoff_location_id,
  d.payment_type,
  pl.payment_type_name AS payment_type_label,
  d.fare_amount,
  d.total_amount,
  d.taxi_type,
  d.extracted_at
FROM deduped d
LEFT JOIN ingestion.payment_lookup pl
  ON CAST(d.payment_type AS VARCHAR) = CAST(pl.payment_type_id AS VARCHAR)
WHERE d.rn = 1
