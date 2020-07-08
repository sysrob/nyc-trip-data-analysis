package blog.iamrob

import org.scalatest.FlatSpec
import blog.iamrob.jobs.TripTop
import blog.iamrob.test.SharedSparkSession.spark

class TripTopTest extends TripTest {

  import spark.implicits._

  val df = toTripDF(Seq(
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 186, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 16.7, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 186, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 16.8, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 186, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 16.9, 2.5),
    
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 9.00, 1, "N", 187, 162, 2, 13, 3, 0.5, 1, 1, 0.3, 16.8, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 7.00, 1, "N", 187, 162, 2, 13, 3, 0.5, 1, 1, 0.3, 16.7, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 12.00, 1, "N", 187, 162, 2, 13, 3, 0.5, 1, 1, 0.3, 17.0, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 50.00, 1, "N", 187, 162, 2, 13, 3, 0.5, 1, 1, 0.3, 17.1, 2.5))) // outlier)

  "transform" should "get top total_amount for each non-outlier distance LocationId pair" in {
    val res = TripTop
      .transform(spark, df)
      .select("PULocationID", "DOLocationID", "total_amount")
      .collect()

    assert(res.length === 2)

    assert(res(0)(0) === 187)
    assert(res(0)(1) === 162)
    assert(res(0)(2) === 17.0)

    assert(res(1)(0) === 186)
    assert(res(1)(1) === 161)
    assert(res(1)(2) === 16.9)
  }

  "removeOutliers" should "remove distance outliers" in {
    val res = TripTop
      .removeOutliers(df)
      .count()
    
    assert(res === 6)
  }

  "addOutlierFlag" should "add outlier flag columns" in {
    val res1 = TripTop
      .addOutlierFlag(df, "trip_distance", Nil)
      .columns
    
    val res2 = TripTop
      .addOutlierFlag(df, "trip_distance", Seq("PULocationID", "DOLocationID"))
      .columns

    assert(res1.contains("trip_distance_out"))
    assert(res2.contains("grp_trip_distance_out"))
  }
}
