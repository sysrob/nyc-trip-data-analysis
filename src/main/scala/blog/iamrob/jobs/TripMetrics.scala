package blog.iamrob.jobs

import blog.iamrob._
import blog.iamrob.storage._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.{Dataset, SparkSession}

object TripMetrics extends SparkJob {

  override def appName: String = "trip metrics"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {

    val tripData = storage.read(config.inputPath, config.inputFormat)

    printMetrics(spark, tripData)
  }

  def printMetrics(spark: SparkSession, data: Dataset[_]): Unit = {
    import spark.implicits._

    data.cache()

    // print schema
    data.printSchema()

    // show approx count distinct values to check possibility for aggregation operations
    data.select(data.columns.map(c => approxCountDistinct(col(c)).alias(f"${c}_count")): _*).show(false)

    // print numbers of nulls
    data.select(data.columns.map(c => sum(col(c).isNull.cast("int")).alias(f"${c}_nulls")): _*).show(false)

    val dataEnhanced = data
      .withColumn("duration_minutes",
                  (col("tpep_dropoff_datetime").cast("long") -
                   col("tpep_pickup_datetime").cast("long")) / 60)
      .withColumn("price_per_distance", expr("total_amount / trip_distance"))
      .withColumn("minutes_per_distance", expr("duration_minutes / trip_distance"))
      .withColumn("price_per_minute", expr("total_amount / duration_minutes"))
      .withColumn("DOLocationID_PULocationID_distance", expr("ABS(DOLocationID - PULocationID)"))

    // describe possibly interesting dimensions
    dataEnhanced.describe("VendorID" ,"PULocationID" ,"DOLocationID" ,"passenger_count", "payment_type").show(false)

    // describe billing information
    dataEnhanced.describe("fare_amount", "extra", "mta_tax", "tip_amount", "total_amount").show(false)
    dataEnhanced.describe("tolls_amount", "improvement_surcharge", "congestion_surcharge").show(false)

    // describe possibly correlating values
    dataEnhanced.describe("trip_distance", "duration_minutes", "price_per_distance", "minutes_per_distance", "price_per_minute").show(false)

    // describe how many events per second were produced
    dataEnhanced
      .withColumn("day", dayofyear(col("tpep_pickup_datetime")))
      .withColumn("hour", hour(col("tpep_pickup_datetime")))
      .groupBy("day", "hour")
      .count()
      .select("day", "hour", "count")
      .withColumn("events_per_sec", expr("count / 60 / 60"))
      .describe("events_per_sec")
      .show(false)

    // src and dst id dimensions metrics
    val dimensionStats = dataEnhanced
      .filter("trip_distance > 0")
      .filter("total_amount > 0")
      .filter("duration_minutes > 0")
      .groupBy("PULocationID" ,"DOLocationID")
      .agg(mean("trip_distance"),
           max("trip_distance"),
           mean("total_amount"),
           max("total_amount"),
           mean("duration_minutes"),
           max("duration_minutes"),
           count(lit(1)).alias("count"))
      .filter("count > 10")

    dimensionStats
      .orderBy(desc("avg(total_amount)"))
      .show(50)

    dimensionStats
      .orderBy(desc("max(total_amount)"))
      .show(50)

    // does trip_distance increases with distance between DOLocationID & PULocationID ids?
    println(f"PULocationID, DOLocationID correlation: ${dataEnhanced.stat.corr("DOLocationID_PULocationID_distance", "trip_distance")}")
    println(f"trip_distance, duration_minutes correlation: ${dataEnhanced.stat.corr("trip_distance", "duration_minutes")}")
    println(f"trip_distance, total_amount correlation: ${dataEnhanced.stat.corr("trip_distance", "total_amount")}")
    println(f"duration_minutes, total_amount correlation: ${dataEnhanced.stat.corr("duration_minutes", "total_amount")}")

    // show how much data in different distance ranges
    val total = data.count()
    val splits = Array(
      Double.NegativeInfinity,
      0.0, 2.5, 5.0, 10.0, 20.0, 30.0,
      40.0, 50.0, 100.0, 150.0, 250.0,
      Double.PositiveInfinity)

    val labels = array(Array(
      "bellow 0", "between 0 and 2.5",
      "between 2.5 and 5", "between 5 and 10",
      "between 10 and 20", "between 20 and 30", "between 30 and 40",
      "between 40 and 50", "between 50 and 100",
      "between 100 and 150", "between 150 and 250",
      "over 250")
      .map(lit): _*)

    val bucketizer = new Bucketizer()
      .setInputCol("trip_distance")
      .setOutputCol("split")
      .setSplits(splits)

    bucketizer.transform(data)
      .groupBy("split")
      .count()
      .orderBy("split")
      .withColumn("trip_distance",
                  element_at(labels, col("split").cast("int") + lit(1)))
      .withColumn("percent", round(col("count") / lit(total), 6))
      .show(false)

    // NOTE: most of the other exploratory work was done via Jupyter notebook.
  }
}
