package blog.iamrob.jobs

import blog.iamrob._
import blog.iamrob.storage.{Storage}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, desc, max, expr, count}

object TripTop extends SparkJob {

  override def appName: String = "trip top"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {

    val tripData = storage.read(config.inputPath, config.inputFormat)
    val res = transform(spark, tripData)

    storage.write(res.limit(10), config.outputPath, config.outputFormat, config.outputMode)
  }

  def transform(spark: SparkSession, data: Dataset[_]): Dataset[_] = {
   /*
     Using a box-and-whisker outlier detection method.      
     Features / combinations of features selected: duration_minutes, trip_distance, total_amount     
     Most of the trip distances are scattered between 5 distance units (~85%).
     We will lose a lot of non-outlier values by using trip_distance as a feature on the whole dataset.
     Same goes to another method: trip_distance > 0 and trip_distance < mean + 3 * stdev      
     Only combined features are used on whole dataset and later on location groups: minutes_per_distance, price_per_distance      
     Raw feature - trip_distance is only used on src and dst location groups.       
     Some of the groups are pretty small < 2 and we cannot rely on having only a few values to determine outliers with this method
     We are using total_amount instead of fare_amount or (total_amount - tips) - making an assumption and treating outlier (big/small) taxes & tips as anomalies
     After looking at data it seems that price and time does not correlate, although distance & price, distance & time does.
     Making an assumption that pricing model is - client pays for distance driven per trip
  */
    import spark.implicits._

    removeOutliers(data)
      .groupBy("PULocationID" ,"DOLocationID")
      .agg(max("total_amount").alias("total_amount"),
           count("total_amount").alias("count"))
      .filter("count > 2")
      .orderBy(desc("total_amount"))
      .drop("count")
  }

  def removeOutliers(data: Dataset[_]): Dataset[_] = {

    val dataRefined = data
      .filter("trip_distance > 0")
      .filter("total_amount > 0")
      .filter("fare_amount > 0")
      .withColumn("duration_minutes",
                  (col("tpep_dropoff_datetime").cast("long") -
                   col("tpep_pickup_datetime").cast("long")) / 60)
      .filter("duration_minutes > 0")
      .withColumn("minutes_per_distance", expr("duration_minutes / trip_distance"))
      .withColumn("price_per_distance", expr("total_amount / trip_distance"))  // do we treat rare, big tips as outliers? (in this case - yes)

    val src_dst_grp = Seq("PULocationID" ,"DOLocationID")
    val groupOutlierFeatures = Seq("minutes_per_distance", "price_per_distance", "trip_distance").map(c => (c, src_dst_grp))
    val datasetOutlierFeatues = Seq("minutes_per_distance", "price_per_distance").map(c => (c, Nil))

    val outlierFeatures = groupOutlierFeatures ++ datasetOutlierFeatues
    val dataWithOutlierFlags = outlierFeatures.foldLeft(dataRefined) { case (acc, (colName,groups)) => addOutlierFlag(acc, colName, groups) }

    dataWithOutlierFlags
      .filter(col("minutes_per_distance_out") === false)
      .filter(col("price_per_distance_out") === false)
      .filter(col("grp_minutes_per_distance_out") === false)
      .filter(col("grp_price_per_distance_out") === false)
      .filter(col("grp_trip_distance_out") === false)
      .select(data.columns.map(col): _*)
  }

  def addOutlierFlag(data: Dataset[_], colName: String, groups: Seq[String]): Dataset[Row] = {
    // could be done with (Window.over + filter) vs (groubpy + antijoin)
    // performance depends on the data - if aggregated data after groupby is small - it can fit into broadcast join and will be faster
    // otherwise SortMergeJoin will happen which is less efficient than Window + Filter
    val src_dst_grp = groups match {
      // for better performance we could use df.stat.approxQuantile to avoid window without partitions
      case Nil => Window.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      case _ => Window.partitionBy(groups.map(col): _*)
    }

    val flag_name_prefix = if (groups == Nil) "" else "grp_"
    val flag_name = f"${flag_name_prefix}${colName}_out"
    val quantiles = expr(f"percentile_approx($colName, array(0.25, 0.75), 10)")

    data
      .withColumn("quantiles", quantiles.over(src_dst_grp))
      .withColumn("q1", col("quantiles").getItem(0))
      .withColumn("q3", col("quantiles").getItem(1))
      .withColumn("iqr", expr("q3 - q1"))
      .withColumn("low", expr("q1 - 1.5 * iqr"))
      .withColumn("high", expr("q3 + 1.5 * iqr"))
      .withColumn(flag_name, expr(f"$colName < low or $colName > high"))
      .select((data.columns ++ Seq(flag_name)).map(col): _*)
  }
}
