/* @bruin

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: Date of the trip start
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human-readable name of the payment type
    primary_key: true
    checks:
      - name: not_null
  - name: total_trips
    type: bigint
    description: Total number of trips
    checks:
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: Total number of passengers
  - name: total_trip_distance
    type: float
    description: Total trip distance in miles
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: float
    description: Average fare amount per trip
  - name: avg_tip_amount
    type: float
    description: Average tip amount per trip
  - name: total_revenue
    type: float
    description: Total revenue (all charges)

custom_checks:
  - name: total_trips_positive
    description: Ensure total_trips count is always positive
    query: |
      SELECT COUNT(*) FROM reports.trips_report
      WHERE total_trips <= 0
    value: 0

@bruin */

SELECT
  DATE(t.tpep_pickup_datetime) AS pickup_date,
  t.taxi_type,
  t.payment_type_name,
  COUNT(*) AS total_trips,
  CAST(SUM(t.passenger_count) AS BIGINT) AS total_passengers,
  SUM(t.trip_distance) AS total_trip_distance,
  AVG(t.fare_amount) AS avg_fare_amount,
  AVG(t.tip_amount) AS avg_tip_amount,
  SUM(t.total_amount) AS total_revenue
FROM staging.trips t
WHERE DATE(t.tpep_pickup_datetime) >= '{{ start_date }}'
  AND DATE(t.tpep_pickup_datetime) < '{{ end_date }}'
GROUP BY
  DATE(t.tpep_pickup_datetime),
  t.taxi_type,
  t.payment_type_name
ORDER BY
  pickup_date,
  taxi_type,
  payment_type_name

