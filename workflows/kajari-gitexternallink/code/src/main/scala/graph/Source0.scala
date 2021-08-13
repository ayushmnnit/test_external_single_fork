package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

@Visual(id = "Source0", label = "Source0", x = 170, y = 50, phase = 0)
object Source0 {

  @UsesDataset(id = "16", version = 0)
  def apply(spark: SparkSession): Source = {
    import spark.implicits._

    val out = Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(Array(StructField("q", StringType, false), StructField("e", StringType, false)))
        spark.read
          .format("parquet")
          .schema(schemaArg)
          .load("http://sdfs")
          .cache()
      case _ => throw new Exception(s"The fabric is not handled")
    }

    out

  }

}
