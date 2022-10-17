
-- Create Dim Table
create table dbo.Dim_Number_of_Week (
Start_date date not null, 
End_date date not null,
Number_of_the_Week smallint not null
)


insert into dbo.Dim_Number_of_Week values 
('2018-01-01','2018-01-06', 1),
('2018-01-07','2018-01-13', 2),
('2018-01-14','2018-01-20', 3),
('2018-01-21','2018-01-27', 4),
('2018-01-28','2018-02-03', 5),
('2018-02-04','2018-02-10', 6),
('2018-02-11','2018-02-17', 7),
('2018-02-18','2018-02-24', 8),
('2018-02-25','2018-03-03', 9),
('2018-03-04','2018-03-10', 10),
('2018-03-11','2018-03-17', 11),
('2018-03-18','2018-03-24', 12),
('2018-03-25','2018-03-31', 13),
('2018-04-01','2018-04-07', 14),
('2018-04-08','2018-04-14', 15),
('2018-04-15','2018-04-21', 16),
('2018-04-22','2018-04-28', 17),
('2018-04-29','2018-05-05', 18),
('2018-05-06','2018-05-12', 19),
('2018-05-13','2018-05-19', 20),
('2018-05-20','2018-05-26', 21),
('2018-05-27','2018-06-02', 22),
('2018-06-03','2018-06-09', 23),
('2018-06-10','2018-06-16', 24),
('2018-06-17','2018-06-23', 25),
('2018-06-24','2018-06-30', 26),
('2018-07-01','2018-07-07', 27),
('2018-07-08','2018-07-14', 28),
('2018-07-15','2018-07-21', 29),
('2018-07-22','2018-07-28', 30),
('2018-07-29','2018-08-04', 31),
('2018-08-05','2018-08-11', 32),
('2018-08-12','2018-08-18', 33),
('2018-08-19','2018-08-25', 34),
('2018-08-26','2018-09-01', 35),
('2018-09-02','2018-09-08', 36),
('2018-09-09','2018-09-15', 37),
('2018-09-16','2018-09-22', 38),
('2018-09-23','2018-09-29', 39),
('2018-09-30','2018-10-06', 40),
('2018-10-07','2018-10-13', 41),
('2018-10-14','2018-10-20', 42),
('2018-10-21','2018-10-27', 43),
('2018-10-28','2018-11-03', 44),
('2018-11-04','2018-11-10', 45),
('2018-11-11','2018-11-17', 46),
('2018-11-18','2018-11-24', 47),
('2018-11-25','2018-12-01', 48),
('2018-12-02','2018-12-08', 49),
('2018-12-09','2018-12-15', 50),
('2018-12-16','2018-12-22', 51),
('2018-12-23','2018-12-29', 52),
('2018-12-30','2019-01-05', 53)






-- With coordinates
declare @lat1 float = 50.0
declare @long1 float = 14.5
declare @lat2 float = 53.5
declare @long2 float = 9.8

declare @minlat float = case when @lat1 > @lat2 then @lat2 else @lat1 end
declare @maxlat float = case when @lat1 > @lat2 then @lat1 else @lat2 end
declare @minlong float = case when @long1 > @long2 then @long2 else @long1 end
declare @maxlong float = case when @long1 > @long2 then @long1 else @long2 end

select region, Number_of_the_Week, count(*) as number_of_trips
from [dbo].[Trips_with_datasource] a
left join [dbo].[Dim_Number_of_Week] b on cast(a.trip_datetime as date) between b.Start_date and b.End_date
where 1=1
and origin_latitude between @minlat and @maxlat
and origin_longitude between @minlong and @maxlong
and destiny_latitude between @minlat and @maxlat
and destiny_longitude between @minlong and @maxlong
group by region, Number_of_the_Week
order by region, Number_of_the_Week





-- By region
select region, Number_of_the_Week, count(*) as number_of_trips
from [dbo].[Trips_with_datasource] a
left join [dbo].[Dim_Number_of_Week] b on cast(a.trip_datetime as date) between b.Start_date and b.End_date
group by region, Number_of_the_Week
order by region, Number_of_the_Week



