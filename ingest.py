from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, split, to_timestamp, round, when, hour, count
import os

def read_file_spark(file):
	
	spark = SparkSession.builder.appName('Jobsity').getOrCreate()

	print("Reading file")
	df = spark.read.csv(file, sep=';', header=True)

	# parsing latitude and longitude
	df_coord =  df.withColumn("clean_origin", regexp_replace("origin_coord","POINT |\(|\)","")) \
					.withColumn("clean_destiny", regexp_replace("destination_coord","POINT |\(|\)",""))

	df_coord2 = df_coord.withColumn('origin_latitude', split(df_coord['clean_origin'], ' ').getItem(1).cast("float")) \
					.withColumn('origin_longitude', split(df_coord['clean_origin'], ' ').getItem(0).cast("float")) \
					.withColumn('destiny_latitude', split(df_coord['clean_destiny'], ' ').getItem(1).cast("float")) \
					.withColumn('destiny_longitude', split(df_coord['clean_destiny'], ' ').getItem(0).cast("float"))

	# similar origins and destiny has to be grouped together, so we are rounding with 1 decimal place
	df_coord3 = df_coord2.withColumn("origin_latitude", round(col('origin_latitude'),1)) \
						.withColumn("origin_longitude", round(col('origin_longitude'),1)) \
						.withColumn("destiny_latitude", round(col('destiny_latitude'),1)) \
						.withColumn("destiny_longitude", round(col('destiny_longitude'),1))
	
	# defining period of the day to group similar trips together
	df_time1 = df_coord3.withColumn("trip_datetime", to_timestamp("datetime", "dd/MM/yyyy HH:mm"))

	df_time2 = df_time1.withColumn("period_of_day", when(hour(col("trip_datetime")) <= 5, "Dawn")
										.when(hour(col("trip_datetime")) <= 11, "Morning")
										.when(hour(col("trip_datetime")) <= 17, "Afternoon")
										.otherwise("Night"))
	
	# droping columns that are not used anymore
	df_bronze = df_time2.drop('origin_coord', 'destination_coord','datetime','clean_origin','clean_destiny')

	# storing 'raw' files for debugging purposes
	df_bronze.write.parquet(file.replace("csv","parquet"))

	df_silver = df_bronze.groupBy('region','origin_latitude', 'origin_longitude', \
							'destiny_latitude', 'destiny_longitude', 'period_of_day').agg(count("*").alias("number_of_trips"))

	
	#writing dataframes into SQL Server tables
	database = "Jobsity"
	jdbc_url = f"jdbc:sqlserver://localhost:1433;databaseName={database};integratedSecurity=true"

	sql_table_silver = "dbo.trips_silver"
	df_silver.write.mode("append") \
		.format("jdbc") \
		.option("url", jdbc_url) \
		.option("dbtable", sql_table_silver) \
		.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
		.save()
		
	sql_table_bronze = "dbo.trips_bronze"
	df_bronze.write.mode("append") \
		.format("jdbc") \
		.option("url", jdbc_url) \
		.option("dbtable", sql_table_bronze) \
		.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
		.save()

	spark.stop()

	# removing the CSV file
	os.remove(file)