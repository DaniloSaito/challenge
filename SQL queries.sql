

-- Weekly average number of trips for an area, defined by a bounding box
declare @lat1 float = 50.0
declare @long1 float = 14.5
declare @lat2 float = 53.5
declare @long2 float = 9.8

declare @minlat float = case when @lat1 > @lat2 then @lat2 else @lat1 end
declare @maxlat float = case when @lat1 > @lat2 then @lat1 else @lat2 end
declare @minlong float = case when @long1 > @long2 then @long2 else @long1 end
declare @maxlong float = case when @long1 > @long2 then @long1 else @long2 end

select sum(number_of_trips) / count(Number_of_the_Week) as Weekly_Average_number_of_trips from (
	select Number_of_the_Week, count(*) as number_of_trips
	from [dbo].[trips_bronze] a
	left join [dbo].[Dim_Number_of_Week] b on cast(a.trip_datetime as date) between b.Start_date and b.End_date
	where 1=1
	and origin_latitude between @minlat and @maxlat
	and origin_longitude between @minlong and @maxlong
	and destiny_latitude between @minlat and @maxlat
	and destiny_longitude between @minlong and @maxlong
	group by Number_of_the_Week
	) a




-- Weekly average number of trips for an area, defined by a region
select region, sum(number_of_trips) / count(Number_of_the_Week) as Weekly_Average_number_of_trips 
from (
	select region, Number_of_the_Week, count(*) as number_of_trips
	from [dbo].[trips_bronze] a
	left join [dbo].[Dim_Number_of_Week] b on cast(a.trip_datetime as date) between b.Start_date and b.End_date
	group by region, Number_of_the_Week
	) b
group by region




-- From the two most commonly appearing regions, which is the latest datasource
select region, datasource as latest_datasource from (
	-- Ranking trips by date
	select *,
	ROW_NUMBER() OVER (PARTITION BY region order by trip_datetime desc) as rank_latest_trip_by_region
	from
	dbo.trips_bronze
	where region in (
		-- 2 most commonly appearing regions
		select top 2 region from
		dbo.trips_bronze
		group by region
		order by count(*) desc
	)
) trips_ranked_by_date
where rank_latest_trip_by_region = 1



-- What regions has the "cheap_mobile" datasource appeared in?
select distinct region
from dbo.trips_bronze
where datasource = 'cheap_mobile'

