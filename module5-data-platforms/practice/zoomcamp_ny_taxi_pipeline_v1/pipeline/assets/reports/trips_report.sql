/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: trip_date
    type: date
    description: Date of the trip (based on pickup_datetime)
    primary_key: true

  - name: taxi_type
    type: varchar
    description: Type of taxi (yellow, green)
    primary_key: true

  - name: payment_type_label
    type: varchar
    description: Payment method name
    primary_key: true

  - name: trip_count
    type: bigint
    description: Total number of trips
    checks:
      - name: positive

  - name: total_passengers
    type: bigint
    description: Total number of passengers
    checks:
      - name: non_negative

  - name: total_distance
    type: double
    description: Total trip distance in miles
    checks:
      - name: non_negative

  - name: total_fare
    type: double
    description: Total fare amount
    checks:
      - name: non_negative

  - name: total_revenue
    type: double
    description: Total revenue (total_amount)
    checks:
      - name: non_negative

  - name: avg_fare
    type: double
    description: Average fare per trip

  - name: avg_trip_distance
    type: double
    description: Average trip distance in miles

  - name: avg_passengers
    type: double
    description: Average passengers per trip

custom_checks:
  - name: row_count_positive
    description: Ensures the report is not empty
    query: SELECT COUNT(*) > 0 FROM reports.trips_report
    value: 1

@bruin */

-- Aggregate trips by date, taxi type, and payment type

SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    payment_type_label,

    -- Count metrics
    COUNT(*) AS trip_count,
    SUM(COALESCE(passenger_count, 0)) AS total_passengers,

    -- Distance metrics
    SUM(COALESCE(trip_distance, 0)) AS total_distance,

    -- Revenue metrics
    SUM(COALESCE(fare_amount, 0)) AS total_fare,
    SUM(COALESCE(total_amount, 0)) AS total_revenue,

    -- Average metrics
    AVG(COALESCE(fare_amount, 0)) AS avg_fare,
    AVG(COALESCE(trip_distance, 0)) AS avg_trip_distance,
    AVG(COALESCE(passenger_count, 0)) AS avg_passengers

FROM staging.trips

WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
  AND COALESCE(fare_amount, 0) >= 0
  AND COALESCE(total_amount, 0) >= 0

GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    payment_type_label