package blog.iamrob

import org.scalatest.FlatSpec
import blog.iamrob.jobs.TripNeighbourhoodTop
import blog.iamrob.test.SharedSparkSession.spark

class TripNeighbourhoodTopTest extends TripTest {

  import spark.implicits._

  val df = toTripDF(Seq(
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 1, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 14.1, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 1, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 14.2, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 1, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 14.3, 2.5),

    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 185, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 15.1, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 185, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 15.2, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 185, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 15.3, 2.5),

    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 186, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 16.7, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 186, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 16.8, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 186, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 16.9, 2.5),

    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 9.00, 1, "N", 187, 162, 2, 13, 3, 0.5, 1, 1, 0.3, 16.8, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 7.00, 1, "N", 187, 162, 2, 13, 3, 0.5, 1, 1, 0.3, 16.7, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 12.00, 1, "N", 187, 162, 2, 13, 3, 0.5, 1, 1, 0.3, 17.0, 2.5),
    (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 50.00, 1, "N", 187, 162, 2, 13, 3, 0.5, 1, 1, 0.3, 17.1, 2.5))) // outlier)

  "transform" should "get top total_amount for each non-outlier distance in LocationId pair per neigbourhood" in {
    val res = TripNeighbourhoodTop
      .transform(spark, df)
      .select("PULocationID", "DOLocationID", "neighbourhood_total_amount", "total_amount")
      .collect()

    assert(res.length === 4)

    assert(res(0)(0) === 187)
    assert(res(0)(1) === 162)
    assert(res(0)(2) === 17.0 && res(0)(3) === 17.0)

    assert(res(1)(0) === 186)
    assert(res(1)(1) === 161)
    assert(res(1)(2) === 17.0 && res(1)(3) === 16.9)

    assert(res(2)(0) === 185)
    assert(res(2)(1) === 161)
    assert(res(2)(2) === 16.9 && res(2)(3) === 15.3)

    assert(res(3)(0) === 1)
    assert(res(3)(1) === 161)
    assert(res(3)(2) === 14.3 && res(3)(3) === 14.3)
  }


  "expandNeighbourhood" should "create unique combinations for nearby (+1, -1, 0) src and dst pairs" in {
    val trips = Seq(
      (5, 5, 10), 
      (112, 25, 10))
    .toDF("src", "dst", "total_amount")

    val res = TripNeighbourhoodTop
      .expandNeighbourhood(trips, "src", "dst")
      .dropDuplicates("src", "dst")
      .filter(($"src".between(4, 6) && $"dst".between(4, 6)) || 
              ($"src".between(111, 113) && $"dst".between(24, 26)))
      .collect()

    assert(res.length === trips.count() * 9)
  }
}
