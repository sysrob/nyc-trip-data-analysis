package blog.iamrob

import org.scalatest.FlatSpec
import org.apache.spark.sql.Row
import org.apache.spark.sql.{Dataset}
import blog.iamrob.test.SharedSparkSession.spark


class TripTest extends FlatSpec {
  def toTripDF(data: Seq[Tuple18[
    Int, String, String, Int, Double, Int, String, Int, Int,
    Int, Int, Int, Double, Int, Int, Double, Double, Double
  ]]): Dataset[Row] = {
    import spark.implicits._

    val cols = Seq(
      "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", 
      "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
      "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax",
      "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge")

    data
      .toDF(cols: _*)
      .withColumn("tpep_pickup_datetime", $"tpep_pickup_datetime".cast("timestamp"))
      .withColumn("tpep_dropoff_datetime", $"tpep_dropoff_datetime".cast("timestamp"))
  }
}