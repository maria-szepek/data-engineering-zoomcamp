# queries
```
-- question 3: Count of records in fct_monthly_zone_revenue? 
select count(1) from taxi_rides_ny.prod.fct_monthly_zone_revenue;

-- question 4: Zone with highest revenue for Green taxis in 2020?
with filtered as (
select
  pickup_zone,
  revenue_month,
  revenue_monthly_total_amount

  -- max(revenue_monthly_total_amount) as max_revenue
  -- extract('year' FROM revenue_month) as year, 
  -- *
from taxi_rides_ny.prod.fct_monthly_zone_revenue
where service_type like 'Green'
and extract('year' FROM revenue_month) = 2020
order by revenue_monthly_total_amount desc
), grouped as (
select 
  pickup_zone,
sum(revenue_monthly_total_amount) as total_in_year, 
from filtered 
group by pickup_zone
)
select * 
from grouped
order by total_in_year desc
limit 10



-- question 5: Total trips for Green taxis in October 2019?
select count(1) 
from taxi_rides_ny.prod.fct_trips
where service_type like 'Green'
  AND pickup_datetime >= '2019-10-01'
  AND pickup_datetime <  '2019-11-01'
-- and extract ('month' from pickup_datetime)
limit 10

-- question 6: Count of records in stg_fhv_tripdata (filter dispatching_base_num IS NULL)?
select count(1)
from taxi_rides_ny.prod.stg_fhv_tripdata
limit 10
```

