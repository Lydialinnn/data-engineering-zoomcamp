# data-engineering-zoomcamp
data-engineering-zoomcamp 2025
# WK3 HOMEWORK:
-- Q1:
SELECT count(1)
FROM `dulcet-record-450703-g3.nyc_taxi.yellow_tripdata_regular`;

-- Q2:
SELECT DISTINCT COUNT(PULocationID)
FROM `dulcet-record-450703-g3.nyc_taxi.yellow_tripdata_regular`;

SELECT DISTINCT COUNT(PULocationID)
FROM `dulcet-record-450703-g3.nyc_taxi.yellow_tripdata_external`;

-- Q3:
SELECT PULocationID, DOLocationID
FROM `dulcet-record-450703-g3.nyc_taxi.yellow_tripdata_regular`;

-- Q4:
SELECT count(*)
FROM `dulcet-record-450703-g3.nyc_taxi.yellow_tripdata_regular`
where fare_amount=0;

-- Q5:
CREATE OR REPLACE TABLE `dulcet-record-450703-g3.nyc_taxi.yellow_tripdata_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `dulcet-record-450703-g3.nyc_taxi.yellow_tripdata_regular`;

-- Q6:
SELECT DISTINCT COUNT(VendorID)
FROM `dulcet-record-450703-g3.nyc_taxi.yellow_tripdata_regular`
WHERE tpep_dropoff_datetime>='2024-03-01' AND tpep_dropoff_datetime<='2024-03-15';

SELECT DISTINCT COUNT(VendorID)
FROM `dulcet-record-450703-g3.nyc_taxi.yellow_tripdata_optimized`
WHERE tpep_dropoff_datetime>='2024-03-01' AND tpep_dropoff_datetime<='2024-03-15';
