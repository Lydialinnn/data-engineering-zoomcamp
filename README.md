# data-engineering-zoomcamp
data-engineering-zoomcamp 2025
## WK5 batch HOMEWORK:
-- Q1:
import pyspark
from pyspark.sql import SparkSession

!spark-shell --version

spark = SparkSession.builder.master("local[1]") \
                    .appName('test-spark') \
                    .getOrCreate()

print(f'The PySpark {spark.version} version is running...')

-- Q2:
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")
df_repartitioned = df.repartition(4)
df_repartitioned.write.parquet("yellow_tripdata_repartitioned.parquet")


-- Q3:
from pyspark.sql.functions import col, to_date
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")
df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))

oct_15th_trips = df.filter(col("pickup_date") == "2024-10-15")

num_trips_oct_15 = oct_15th_trips.count()

print(f"Number of taxi trips on the 15th of October: {num_trips_oct_15}")

-- Q4:
from pyspark.sql.functions import col, unix_timestamp
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")
df = df.withColumn("trip_duration_seconds", 
                  unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime"))

df = df.withColumn("trip_duration_hours", col("trip_duration_seconds") / 3600)

longest_trip_duration = df.agg({"trip_duration_hours": "max"}).collect()[0][0]

print(f"The longest trip duration is {longest_trip_duration:.2f} hours.")

-- Q5:
4040

-- Q6:
from pyspark.sql.functions import col

joined_df = spark.sql("""
    SELECT 
        z.Zone AS pickup_zone, 
        COUNT(*) AS pickup_count
    FROM 
        yellow_trip_data y
    JOIN 
        zone_lookup z
    ON 
        y.PULocationID = z.LocationID
    GROUP BY 
        z.Zone
    ORDER BY 
        pickup_count ASC
    LIMIT 1
""")

joined_df.show()
