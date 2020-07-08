package blog.iamrob.jobs

import blog.iamrob._
import blog.iamrob.storage._
import org.apache.spark.sql.{Dataset, SparkSession, DataFrameWriter}
import org.apache.spark.sql.functions.{col, desc, max, expr}

object TripDownload extends SparkJob {

  override def appName: String = "NYC trip data download"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {
    val tripData = storage.read(config.inputPath, config.inputFormat)
    val enhancedData = filterOutlierPartitions(spark, tripData)

    // Partitions will give us an ability to control schema evolution
    // Possible to turn it on read with read.<...>.option("mergeSchema", "true")
    // Since it's an expensive operation, currently we will keep it turned off
    // For example on datasets between 2015 and 2018 there is a small schema change:
    // Removed columns: 'dropoff_longitude', 'dropoff_latitude', 'pickup_longitude', 'pickup_latitude'
    // New columns: 'PULocationID', 'DOLocationID'

    val setupWriter = (x: DataFrameWriter[_]) => x.partitionBy("year", "month")

    storage.write(
      enhancedData,
      config.outputPath,
      config.outputFormat,
      config.outputMode,
      setupWriter)
  }

  def filterOutlierPartitions(spark: SparkSession, data: Dataset[_]) = {
    import spark.implicits._

    // Filter out data which does not belong to the year and month (there are some outliers in most cases)
    // Another approach would be to update the inccorect dates
    val dataWithPartitions = data
      .withColumn("year",  expr("year(tpep_pickup_datetime)"))
      .withColumn("month", expr("month(tpep_pickup_datetime)"))

    val topYear = getTopValueByCount(dataWithPartitions, "year")
    val topMonth = getTopValueByCount(dataWithPartitions, "month")

    dataWithPartitions
      .where(f"year = ${topYear} AND month = ${topMonth}")
  }

  def getTopValueByCount(data: Dataset[_], column: String) = {
    data
      .groupBy(column)
      .count()
      .orderBy(desc("count"))
      .limit(1)
      .collect()(0)(0)
  }
}
