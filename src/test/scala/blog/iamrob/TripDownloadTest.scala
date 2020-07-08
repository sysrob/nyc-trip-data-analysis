package blog.iamrob

import org.scalatest.FlatSpec
import blog.iamrob.jobs.TripDownload
import blog.iamrob.test.SharedSparkSession.spark

class TripDownloadTest extends TripTest {
  import spark.implicits._

  "filterOutlierPartitions" should "leave only one partition for year and month based on most occurences " in {
    val data = Seq(
        ("2019-09-01 00:06:48"), ("2019-09-01 00:06:48"),
        ("2019-09-01 00:06:48"), ("2019-09-01 00:06:48"),
        ("2019-09-01 00:06:48"), ("2019-09-01 00:06:48"), 
        ("2017-08-01 00:06:48"), ("2015-04-01 00:06:48"))
      .toDF("tpep_pickup_datetime")
      .withColumn("tpep_pickup_datetime", $"tpep_pickup_datetime".cast("timestamp"))

    val res = TripDownload.filterOutlierPartitions(spark, data)
    val rows = res.collect()

    assert(res.count() === 6)
    assert(res.dropDuplicates("year", "month").count() === 1)
    
    assert(rows(0)(1) === 2019)
    assert(rows(0)(2) === 9)
  }

  "getTopValueByCount" should "get top value by number of occurences in dataset" in {
    val data = Seq((1), (2), (3), (4), (4), (3), (3)).toDF("month")
    val res = TripDownload.getTopValueByCount(data, "month")

    assert(res === 3)
  }
}
