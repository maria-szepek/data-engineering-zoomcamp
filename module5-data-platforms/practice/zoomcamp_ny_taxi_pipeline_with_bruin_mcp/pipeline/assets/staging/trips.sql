/* @bruin

name: staging.trips

type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: tpep_pickup_datetime
  time_granularity: timestamp

columns:
  - name: vendor_id
    type: integer
    description: A code indicating the TLCB-licensed taxi provider
    checks:
      - name: not_null
  - name: tpep_pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged
    primary_key: true
    checks:
      - name: not_null
  - name: tpep_dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged
    primary_key: true
    checks:
      - name: not_null
  - name: passenger_count
    type: float
    description: The number of passengers in the vehicle
  - name: trip_distance
    type: float
    description: The elapsed trip distance in miles reported by the taximeter
    checks:
      - name: non_negative
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
  - name: payment_type_id
    type: integer
    description: Unique identifier for payment type
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human-readable name of the payment type
    checks:
      - name: not_null
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
    description: Tip amount
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
    checks:
      - name: not_null

custom_checks:
  - name: no_future_dates
    description: Ensure tpep_pickup_datetime is not in the future
    query: |
      SELECT COUNT(*) FROM staging.trips
      WHERE tpep_pickup_datetime > NOW()
    value: 0

@bruin */

WITH deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance
      ORDER BY payment_type DESC
    ) AS rn
  FROM ingestion.trips
  WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
    AND tpep_pickup_datetime < '{{ end_datetime }}'
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND trip_distance > 0
    AND passenger_count > 0
)
SELECT
  d.vendor_id,
  d.tpep_pickup_datetime,
  d.tpep_dropoff_datetime,
  d.passenger_count,
  d.trip_distance,
  d.ratecode_id,
  d.store_and_fwd_flag,
  d.pu_location_id,
  d.do_location_id,
  COALESCE(p.payment_type_id, d.payment_type) AS payment_type_id,
  COALESCE(p.payment_type_name, 'unknown') AS payment_type_name,
  d.fare_amount,
  d.extra,
  d.mta_tax,
  d.tip_amount,
  d.tolls_amount,
  d.improvement_surcharge,
  d.total_amount,
  d.congestion_surcharge,
  d.airport_fee,
  d.taxi_type
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
  ON d.payment_type = p.payment_type_id
WHERE d.rn = 1


