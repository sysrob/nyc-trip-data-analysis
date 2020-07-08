package blog.iamrob.storage


import scala.io.Source._
import org.apache.spark.sql.Row
import org.apache.spark.sql.{Encoder, SparkSession, Dataset, DataFrameWriter}

trait Storage {
  def read(path: String, format: String): Dataset[Row]
  def write(
    ds: Dataset[_], path: String,
    format: String, mode: String,
    setupWriter: DataFrameWriter[_] => DataFrameWriter[_] = x => x)
}

class LocalStorage(spark: SparkSession) extends Storage {
  import spark.implicits._

   override def read(path: String, format: String): Dataset[Row] = {
    format match {
      case "csv" => readCsv[Row](path)
      case "api-csv" => downloadCsv[Row](path)
      case _ => readAny(path, format)
    }
  }

  override def write(
    ds: Dataset[_], path: String,
    format: String, mode: String,
    setupWriter: DataFrameWriter[_] => DataFrameWriter[_]) : Unit = {

    format match {
      case "" => ds.show()
      case _ => {
        val writer = ds.write.format(format).mode(mode)
        setupWriter(writer).save(path)
      }
    }
  }

  private def readCsv[T](path: String)= {
    getCsvReader().csv(path)
  }

  private def downloadCsv[T](url: String) = {
    val res = fromURL(url).mkString.stripMargin.lines.toList
    val csvData = spark.sparkContext.parallelize(res).toDS()
    getCsvReader().csv(csvData)
  }

  private def readAny[T](path: String, format: String) = {
    spark.read.format(format).load(path)
  }

  private def getCsvReader() = {
    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
  }
}
