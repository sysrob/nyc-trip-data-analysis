package blog.iamrob

import blog.iamrob.jobs._
import org.scalatest.FlatSpec
import org.apache.spark.sql.Row
import blog.iamrob.storage.{Storage}
import blog.iamrob.test.SharedSparkSession.spark
import org.apache.spark.sql.{Dataset, SparkSession, DataFrameWriter}


class SparkJobTestIT extends FlatSpec {

  class TestStorage(spark: SparkSession) extends Storage {
    import spark.implicits._

    override def read(path: String, format: String): Dataset[Row] = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv(path)

    override def write(
      ds: Dataset[_], path: String,
      format: String, mode: String,
      setupWriter: DataFrameWriter[_] => DataFrameWriter[_]): Unit = {}
  }

  val config = UsageConfig("src/it/resources/tripdata.csv", "csv")

  "TripDownload" should "read and write to disk" in {
    TripDownload.run(spark, config, new TestStorage(spark))
  }

  "TripMetrics" should "read from disk" in {
    TripMetrics.run(spark, config, new TestStorage(spark))
  }

  "TripTop" should "read and write to disk" in {
    TripTop.run(spark, config, new TestStorage(spark))
  }

  "TripNeighbourhoodTop" should "read and write to disk"  in {
    TripNeighbourhoodTop.run(spark, config, new TestStorage(spark))
  }
}