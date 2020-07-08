## NYC taxi trip records analysis

This project represents Spark jobs which analyze and aggregate NYC taxi trips publicly available data.  
You can download data from here: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page  
Five Yellow taxi trip records datasets (> 2GB) were used for development and testing.  

![master-status](https://github.com/sysrob/nyc-trip-data-analysis/workflows/master/badge.svg)
### Jobs

1. `blog.iamrob.jobs.TripDownload`  
   Downloads NYC taxi trips publicly available data set in csv format (from provided URL)  
   Partitions data by `year` and `month` for easier future schema evolution  
   Writes result to specified location (e.g. hive external table location)  
   For example 2015 and 2018 year data has small schema change: -4 and +2 columns  
   If partitions were changed `MSCK REPAIR TABLE` has to be run to repair them

2. `blog.iamrob.jobs.TripMetrics` 

   Calculates metrics & dimensions to understand and get familiar with the dataset.  
   Sample outputs: [TripMetrics.txt](https://github.com/sysrob/nyc-trip-data-analysis/blob/master/output/TripMetrics.txt)  
   Note: most of the exploration was done via Jupyter notebooks

3. `blog.iamrob.jobs.TripTop`  

   Removes trip_distance outliers by using combined dataset features with box-and-whisker outlier detection method.  
   Calculates top 10 PULocationId, DOLocationId pairs for total_amount.  
   Sample outputs: [TripTop.txt](https://github.com/sysrob/nyc-trip-data-analysis/blob/master/output/TripTop.txt)  
   Data stats before & after outlier removal: [Outliers.txt](https://github.com/sysrob/nyc-trip-data-analysis/blob/master/output/Outliers.txt)

4. `blog.iamrob.jobs.TripNeighbourhoodTop`  

   Calculates the same as previous job, but assumes the pair includes values from neighboured numbers,  
   i.e. pair (5,5) includes (4,4), (4,5), (5,4), (5,5), (4,6), (6,4), (5,6), (6,5), (6,6)  
   Sample outputs: [TripNeighbourhoodTop.txt](https://github.com/sysrob/nyc-trip-data-analysis/blob/master/output/TripNeighbourhoodTop.txt)


#### Outlier Detection Results
Comparison of how well estimated price fits in raw and filtered (trip_distance outliers removed) trip data by using linear least squares method for estimating the unknown parameters (price) in a linear regression model.  
[Notebook](https://github.com/sysrob/nyc-trip-data-analysis/blob/master/notebooks/price_fit_raw_vs_filtered.ipynb)

![price_fit_raw_vs_filtered](https://github.com/sysrob/nyc-trip-data-analysis/blob/master/images/price_fit_raw_vs_filtered.PNG)

#### Prerequisites

Scala 2.11.11 https://www.scala-lang.org/download/  
Spark 2.4.0 https://spark.apache.org/downloads.html  
NYC taxi trips publicly available data: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page 

## Instructions

To build and create JARs, please, run:
```
sbt assembly
```
To run integration tests, please, run:
```
sbt it:test
```
To run unit tests, please, run:
```
sbt test
```
To run spark job, please, run:  
<pre>
spark-submit \
--class blog.iamrob.jobs.<b>CLASS_NAME</b> \
--driver-memory 12g \
--master local[*] \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" \
./target/scala-2.11/nyc-trip-data-analysis-assembly-0.1.0.jar \
--input-path <b>INPUT_PATH</b> \
--input-format <b>INPUT_FORMAT</b> \
--output-path <b>OUTPUT_PATH</b> \
--output-format <b>OUTPUT_FORMAT</b> \
--output-mode <b>OUTPUT_MODE</b>
</pre>

**NOTE:**  
1. **Use** `^` instead of `\` for new lines in Windows CMD environment  
2. **Replace** `CLASS_NAME` with one of the job class name: `TripDownload`, `TripMetrics`, `TripTop`, `TripNeighbourhoodTop`  
3. **Replace** `INPUT_PATH` with path to the data file: online-api, local machine, hdfs or any other cluster mounted location (i.e. `C:/data/yellow_tripdata_2019-09.csv`, `hdfs://...`, `https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2019-09.csv `)  
4. **Replace** `INPUT_FORMAT` with the following: `orc`, `avro`, `parquet`, `csv` or `api-csv` for `TripDownload job`
5. **Replace** `OUTPUT_PATH` with path to your external hdfs table path or any other mounted location
6. **Replace** `OUTPUT_FORMAT` with the following: `orc`, `avro`, `parquet`
7. **Replace** `OUTPUT_MODE` with the following: `overwrite` or `append`
8. **IMPORTANT**: If `--deploy-mode cluster` data file has to be loaded to HDFS or any other mounted location
9. **If no output-<>** variables specified - will write to stdout

### Sample scripts:  
1. [TripDownload](https://github.com/sysrob/nyc-trip-data-analysis/blob/master/scripts/run_1.sh)
2. [TripMetrics](https://github.com/sysrob/nyc-trip-data-analysis/blob/master/scripts/run_2.sh)
3. [TripTop](https://github.com/sysrob/nyc-trip-data-analysis/blob/master/scripts/run_3.sh)
4. [TripNeighbourhoodTop](https://github.com/sysrob/nyc-trip-data-analysis/blob/master/scripts/run_4.sh)

Parameters:
```
--class  
  The entry point for your application
  Example: blog.iamrob.jobs.TripMetrics
  Required: true

--driver-memory 12g
  Set driver memory
  Required: false

--master local[*] 
  Run Spark locally with as many worker threads as logical cores on your machine
  Note: for local run only
  Required: false

--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" 
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" 
  Suppresses spark execution INFO messages in console
  Required: false

--input-path
  Path or Url to the input data, hdfs or any other mounted location
  Required: true

--input-format
  Format of the input data
  Supported values: orc, avro, parquet, csv, api-csv
  Required: true

--output-path  
  Path to the output data, hdfs or any other mounted location
  Required: false

--output-format
  Format of the output data
  Allowed values: orc, avro, parquet
  Required: false

--output-mode
  Write mode
  Allowed values: overwrite, append
  Required: false
```

More about spark-submit parameters: http://spark.apache.org/docs/latest/submitting-applications.html