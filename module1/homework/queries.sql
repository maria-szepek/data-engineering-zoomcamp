-- Question 3. Question 3. For the trips in November 2025, how many trips had
-- a trip_distance of less than or equal to 1 mile? (1 point)
select 
	count(*)
from public."green_tripdata_2025-11"
where 
	lpep_pickup_datetime::date >= '2025-11-01' and 
	lpep_pickup_datetime < '2025-12-01' and 
	trip_distance <= 1

-- Question 4. Which was the pick up day with the longest trip distance?
-- Only consider trips with trip_distance less than 100 miles. (1 point)

select 
	trip_distance,
	lpep_pickup_datetime::date
from public."green_tripdata_2025-11"
where 
	trip_distance < 100
order by trip_distance desc
limit 10

-- Question 5. Which was the pickup zone with the largest total_amount (sum of all trips) 
-- on November 18th, 2025? (1 point)


select
	zd."Zone",	
	round(sum(td.total_amount)::numeric, 2) as sum_total_amount
from public."green_tripdata_2025-11" td
join public.taxi_zone_lookup zd on td."PULocationID" = zd."LocationID"  
where lpep_pickup_datetime::date = '2025-11-18'
group by 1
order by 2 desc

-- Question 6. For the passengers picked up in the zone named -- "East Harlem North" 
-- in November 2025, which was the drop off zone that had the largest tip? (1 point)

select
	td.tip_amount as max_tip_amount,
	dozd."Zone"
from public."green_tripdata_2025-11" td
join public.taxi_zone_lookup puzd on td."PULocationID" = puzd."LocationID"  
join public.taxi_zone_lookup dozd on td."DOLocationID" = dozd."LocationID"  
where 
	puzd."Zone" like 'East Harlem North' and 
	extract (year from td.lpep_pickup_datetime) = 2025 and 
	extract (month from td.lpep_pickup_datetime) = 11	
order by td.tip_amount desc
limit 1
