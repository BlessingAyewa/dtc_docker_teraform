-- Counting short trips
select 
	count(*)
from green_trip
where date(lpep_pickup_datetime) >= '2025-11-01' 
	and date(lpep_pickup_datetime) < '2025-12-01'
	and trip_distance <= 1
; -- Answer: 8007


-- Longest trip for each day
select 
	date(lpep_pickup_datetime) as lpep_pickup_datetime,
	trip_distance 
from green_trip
where 
	trip_distance < 100
order by 
	trip_distance desc; -- Answer: 2025-11-14


-- Biggest pickup zone
select 
	z."Zone",
	sum(g."total_amount") as total_amount
from green_trip as g
	left join zone_lookup as z
	on g."PULocationID" = z."LocationID"
where 
	date(g.lpep_pickup_datetime) = '2025-11-18'
group by 
	z."Zone"
order by 
	total_amount desc; -- Answer: East Harlem North


--  Largest tip
with pu as (
	select 
		*
	from green_trip as g
		left join zone_lookup as z
		on g."PULocationID" = z."LocationID"
	where 
		z."Zone" = 'East Harlem North'
		and date_trunc('month', lpep_pickup_datetime) = '2025-11-01'
)
select 
	z."Zone",
	pu.tip_amount
from pu
	left join zone_lookup as z
	on pu."DOLocationID" = z."LocationID" 
order by 
	pu.tip_amount desc; -- Yorkville West

