package blog.iamrob.jobs

import blog.iamrob._
import blog.iamrob.storage.{LocalStorage, Storage}
import org.apache.spark.sql.{Dataset, SparkSession}

trait SparkJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()

    parseAndRun(spark, args)

    def parseAndRun(spark: SparkSession, args: Array[String]): Unit = {

      new UsageOptionParser().parse(args, UsageConfig()) match {
        case Some(config) =>  run(spark, config, new LocalStorage(spark))
        case None => throw new IllegalArgumentException("arguments provided to job are not valid")
      }
    }
  }

  def run(spark: SparkSession, config: UsageConfig, storage: Storage): Unit

  def appName: String

}
