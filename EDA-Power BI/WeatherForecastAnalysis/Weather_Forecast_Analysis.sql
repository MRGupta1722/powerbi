/* TASK 1: 
Give the count of the minimum number of days for the time when temperature reduced
Logic: Current day's maximum temperature <= yesterday's minimum temperature 
This will give us the atleast for these many days the temperature has been reduced/dropped

*/

WITH T1 AS(
		select date as date_new ,  maximum_temperature, minimum_temperature,
		maximum_temperature - LAG(minimum_temperature) OVER(ORDER BY date)  as max_min_temp_diff
		from dbo.final_weather_data

		), 
T2 AS (
	select date_new, max_min_temp_diff
	from T1 
	where max_min_temp_diff<=0
	)

select count(T2.date_new) as min_count_when_temp_reduced
from T1
join T2
on T1.date_new = T2.date_new;



/* TASK 2: 
Find the temperature as Cold / hot by using the case and avg of values of the given data set
*/

Declare @avg_temp as int;
set @avg_temp = (select AVG(average_temperature) from dbo.weather_final);

Select date, average_temperature as Temp_in_Fahr, 
case 
 when average_temperature < @avg_temp then 'Cold'
 when average_temperature > @avg_temp then 'hot'
 else 'Perfect'
 end as Temp_status
 
from dbo.final_weather_data
group by date, average_temperature order by date
;

/* TASK 3: 
Can you check for all 4 consecutive days when the temperature was below 30 Fahrenheit
Logic: Will be considering minimum temperature to be below 30F as it means that 
during some point of time during the day the temperature  was below 30F
*/

WITH CTE as(
select  RANK() OVER (ORDER BY date) as row_number,
	date, minimum_temperature,
DATEADD(d, - RANK() Over ( order by date), date) as date_group
from dbo.final_weather_data
where minimum_temperature<30 
), 
tableA  AS(
	select count(*) as count_streak, 
	MIN(date) as min_date, MAX(date) as max_date from CTE 
	group by date_group
) 

select tba.min_date , tba.max_date, tba.count_streak, w.minimum_temperature
from tableA as tba 
join dbo.final_weather_data as w
on tba.min_date = w.date
where count_streak=4;


/* TASK 4:
Can you find the maximum number of days for which temperature dropped

Logic: If we find "The minimum number of days when the temperature was increased";
which is 0% chance that the temperature was dropped and 
if we subtract this number from the total then we will get the 
maximum number of days when the temperature was reduced.
*/

WITH T3 AS(
		select date ,  maximum_temperature, minimum_temperature,
		minimum_temperature - LAG(maximum_temperature) OVER(ORDER BY date)  as min_max_temp_diff
		from dbo.final_weather_data
),
T4 AS (
	select date, min_max_temp_diff
	from T3
	where min_max_temp_diff>=0
	)

select count(T4.date) as count_when_temp_increased, 
(SELECT count(*) as total_count from dbo.final_weather_data) - count(T4.date) as max_count_when_reduced
from T3
join T4
on T3.date = T4.date;



/* TASK 5: 
Can you find the average humidity average from the dataset 
To use it in Query 5 Adding new column Month and Year and inserting values into it 
*/

Alter table dbo.final_weather_data
Add month_extracted int null;
Alter table dbo.final_weather_data
Add year_extracted int null;
update dbo.final_weather_data set month_extracted = MONTH(date);
update dbo.final_weather_data set year_extracted = YEAR(date);


SELECT  year_extracted, month_extracted,  AVG(average_humidity) AS Avg_humidity_in_Perc 
FROM dbo.final_weather_data
GROUP BY year_extracted, month_extracted
ORDER BY year_extracted, month_extracted
;

/* TASK 6:
Use the GROUP BY clause on the Date column and make a query 
to fetch details for average windspeed ( which is now windspeed done in task 3 )

Assuming that 4 consecutive days have less than 30F;
Will now group their avg(average_windspeed) based on the above condition
*/
WITH temp_table_1 as(
select  
date, minimum_temperature, average_windspeed,
DATEADD(d, - RANK() Over ( order by date), date) as date_group
from dbo.final_weather_data
where minimum_temperature<30 
)
SELECT count_streak, date_group,avg_windspeed, min_date, max_date
FROM(
		select count(*) as count_streak, date_group,  
			round(avg(average_windspeed),2) as avg_windspeed, 
			MIN(date) as min_date, 
			MAX(date) as max_date 
			from  temp_table_1
			group by date_group
	) as temp_table_2
where count_streak =4
order by date_group;

/*TASK 7:
Please add the data in the dataset for 2034 and 2035 as 
well as forecast predictions for these years -> Added using python
 */


 /* Task 8: 
If the maximum gust speed increases from 55mph, fetch the details for the next 4 days
*/

create table weather(
  date1 date,
  Temperature1 float,
  maximum_gust_speed float
);
Declare @date1 date
Declare @temp float
Declare @max_gust_speed float

declare @date_row date
declare @four_days date


 Declare Row_cursor1 cursor for
  select date,average_temperature,maximum_gust_speed from dbo.final_weather_data
	where maximum_gust_speed>55
  open Row_Cursor1
  Fetch next from Row_cursor1 into @date1,@temp, @max_gust_speed    
	while (@@FETCH_STATUS =0)

   		Begin        
			If @max_gust_speed >55
        	set @date_row= (Select date from dbo.final_weather_data where date =DATEADD(day,1,@date1))
        	set @four_days = (Select date from dbo.final_weather_data where date = DATEADD(day,4,@date_row))
        		while (@date_row < @four_days)
          			begin
          				select @date1 = date ,@temp=average_temperature,@max_gust_speed = maximum_gust_speed from dbo.final_weather_data where date = @date_row
          			Insert into weather(date1, Temperature1, maximum_gust_speed) values (@date1,@temp,@max_gust_speed)
          				set @date_row = (Select date from dbo.final_weather_data where date =DATEADD(day,1,@date_row))
        			End       
				Fetch next from Row_cursor1 into @date1,@temp, @max_gust_speed
      while (@date1< @four_days)
        begin
         Fetch next from Row_cursor1 into @date1,@temp, @max_gust_speed
        End 
   End
   Close Row_Cursor1
   Deallocate Row_Cursor1


Select * from weather;

/*TASK 9:
Find the number of days when the temperature went below 0 degrees Celsius 

*/

Alter table dbo.final_weather_data
Add min_temperature_celcius int null;

update dbo.final_weather_data set min_temperature_celcius= ((dbo.final_weather_data.minimum_temperature -32)*5)/9;


WITH Temp_C AS (
	select date, min_temperature_celcius
	from dbo.final_weather_data
	where min_temperature_celcius<0
	)

select count(Temp_C.min_temperature_celcius) as count_when_min_temp0
from Temp_C
join dbo.final_weather_data as w
on Temp_C.date = w.date;



/*Task 10:
Create another table with a “Foreign key” relation with the existing given data set.
*/
/*Step1: will declare Date column as not null and assign a primary key to it*/



ALTER TABLE dbo.final_weather_data
ADD PRIMARY KEY (date);

/*Code to create a duplicate column with existing data and assigning DATE column foreign key */
USE [Weather_Forecast_Analysis]
GO

/****** Object:  Table [dbo].[weather_data_1]    Script Date: 02/11/2023 10:26:20 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[weather_data_1](
	[date] [date] NOT NULL FOREIGN KEY (date) references dbo.final_weather_data(date), 
	[average_temperature] [real] NULL,
	[average_humidity] [real] NULL,
	[average_dewpoint] [real] NULL,
	[average_barometer] [real] NULL,
	[average_windspeed] [real] NULL,
	[average_gustspeed] [real] NULL,
	[average_direction] [real] NULL,
	[rainfall_for_month] [real] NULL,
	[rainfall_for_year] [real] NULL,

) ON [PRIMARY]
GO



/*Code to create a duplicate column with existing data and assigning DATE column foreign key */
USE [Weather_Forecast_Analysis]
GO

/****** Object:  Table [dbo].[weather_data_2]    Script Date: 02/11/2023 10:26:20 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[weather_data_2](
	[date] [date] NOT NULL FOREIGN KEY (date) references dbo.final_weather_data(date),
	[maximum_rain_per_minute] [real] NULL,
	[maximum_temperature] [real] NULL,
	[minimum_temperature] [real] NULL,
	[maximum_humidity] [real] NULL,
	[minimum_humidity] [float] NULL,
	[maximum_pressure] [real] NULL,
	[minimum_pressure] [real] NULL,
	[maximum_windspeed] [real] NULL,
	[maximum_gust_speed] [real] NULL,
	[maximum_heat_index] [real] NULL,

) ON [PRIMARY]
GO

/* Inserting values in the created tables */

/* Table1: weather_data_1 */

INSERT into dbo.weather_data_1
	SELECT date,
		average_temperature ,
		average_humidity,
		average_dewpoint,
		average_barometer,
		average_windspeed,
		average_gustspeed,
		average_direction,
		rainfall_for_month,
		rainfall_for_year
 
 FROM dbo.final_weather_data;

 /*Table 2: weather_data_2*/

 INSERT INTO dbo.weather_data_2
	SELECT	date, 
		maximum_rain_per_minute,
		maximum_temperature,
		minimum_temperature,
		maximum_humidity,
		minimum_humidity,
		maximum_pressure,
		minimum_pressure,
		maximum_windspeed,
		maximum_gust_speed,
		maximum_heat_index
	FROM dbo.final_weather_data;