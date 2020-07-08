package blog.iamrob

import org.scalatest.FlatSpec
import blog.iamrob.jobs.TripMetrics
import blog.iamrob.test.SharedSparkSession.spark

class TripMetricsTest extends TripTest {

  import spark.implicits._

  val df = toTripDF(Seq( 
   (1, "2019-09-01 00:06:48", "2019-09-01 00:25:46", 1, 2.00, 1, "N", 186, 161, 2, 13, 3, 0.5, 1, 1, 0.3, 16.7, 2.5)))

  "printMetrics" should "print metrics" in {
    TripMetrics.printMetrics(spark, df)
  }
}