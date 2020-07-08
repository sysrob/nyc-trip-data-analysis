spark-submit ^
--class blog.iamrob.jobs.TripTop ^
--driver-memory 12g ^
--master local[*] ^
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" ^
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:./log4j.properties" ^
./target/scala-2.11/nyc-trip-data-analysis-assembly-0.1.0.jar ^
--input-path C:/data/bronze/nyc_yellow_taxi/ ^
--input-format orc ^
--output-path C:/data/gold/nyc_yellow_taxi_trip_top/ ^
--output-format orc ^
--output-mode overwrite