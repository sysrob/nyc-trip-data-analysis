package blog.iamrob.jobs

import blog.iamrob._
import blog.iamrob.storage._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, desc, max, expr}

object TripNeighbourhoodTop extends SparkJob {

  override def appName: String = "trip neighbourhood top"

  override def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit = {

    val data = storage.read(config.inputPath, config.inputFormat)
    val res = transform(spark, data)

    storage.write(res.limit(10), config.outputPath, config.outputFormat, config.outputMode)
  }

  def transform(spark: SparkSession, data: Dataset[_]): Dataset[_] = {
    import spark.implicits._

    // cache dataset since it will be reused multiple times
    val topTrips = TripTop.transform(spark, data).cache()
    expandNeighbourhood(topTrips, "PULocationID", "DOLocationID")
      .groupBy("PULocationID", "DOLocationID")
      .agg(max("total_amount").alias("neighbourhood_total_amount"))
      .join(topTrips, usingColumns = Seq("PULocationID", "DOLocationID"), "right")
      .orderBy(desc("neighbourhood_total_amount"), desc("total_amount"))
  }

  def expandNeighbourhood(data: Dataset[_], src: String, dst: String): Dataset[_] = {

    List.fill(3)(-1 to 1)
    .flatten.combinations(2)
    .flatMap(_.permutations)
    .map{ case List(a,b) =>
        data.withColumn(src, expr(f"$src + ($a)"))
            .withColumn(dst, expr(f"$dst + ($b)"))}
    .reduce(_ union _)
  }
}
