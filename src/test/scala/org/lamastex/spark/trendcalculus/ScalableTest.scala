package org.lamastex.spark.trendcalculus

import org.scalatest._
import matchers.should._
import scala.language.postfixOps

class ScalableTest extends SparkSpec with Matchers {

  // Test that Brent Trend Calculus works as expected
  sparkTest("Scalable Trend Calculus") { spark => 

    import org.apache.spark.sql.functions._
    import spark.implicits._

    import org.lamastex.spark.trendcalculus._

    val filePathRoot: String = "src/test/resources/org/lamastex/spark/trendcalculus/"
    val df = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePathRoot+"brent.csv")
      .filter(year(col("DATE")) >= 2016)

    def stringToTimestampUDF = udf((s: String) => new java.sql.Timestamp(new java.text.SimpleDateFormat("yyyy-MM-dd").parse(s).getTime))

    val pointDS = df
      .withColumn("ticker", lit("brent"))
      .withColumn("x", stringToTimestampUDF($"DATE"))
      .select($"ticker", $"x" , $"VALUE" as "y")
      .as[TickerPoint]
      .rdd
      .repartition(1000)
      .toDS
      .orderBy($"x")

    pointDS.show

    val windowSize = 2
    val n = 3

    val tc = new TrendCalculus2(pointDS, windowSize, spark)
    val tc2 = new TrendCalculus2(pointDS, windowSize, spark, false)

    val reversalTS = tc.reversals
    val reversalTS2 = tc2.reversals
    reversalTS.orderBy($"tickerPoint.x").show(false)
    reversalTS2.orderBy($"tickerPoint.x").show(false)

  }

  // Test that stream also works
  sparkTest("Streamable Trend Calculus") { spark =>

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.{Trigger}
    import spark.implicits._
    import sys.process._

    import org.lamastex.spark.trendcalculus._

    val sourceSchema = new StructType().add("DATE", "timestamp").add("VALUE", "double")
    val windowSize = 2
    val n = 3

    val filePathRoot: String = "src/test/resources/org/lamastex/spark/trendcalculus/"
    val df = spark
      .readStream
      .option("header", "true")
      .schema(sourceSchema)
      .csv(filePathRoot+"brent*.csv")
      .filter(year(col("DATE")) >= 2016)

    val pointDS = df.withColumn("ticker", lit("brent")).select($"ticker", $"DATE" as "x", $"VALUE" as "y").as[TickerPoint]

    val parquetPath = "src/test/tmp/parquet"
    val checkpointPath = "src/test/tmp/parquet"

    val testStream = new TrendCalculus2(pointDS, windowSize, spark)
      .reversals
      .writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", parquetPath)
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.Once())
      .start

    testStream.processAllAvailable()
    val tickerSchema = new StructType().add("ticker", "string").add("x", "timestamp").add("y", "double")
    val reversalSchema = new StructType().add("tickerPoint", tickerSchema).add("reversal", "int")

    spark.read.schema(reversalSchema).parquet(parquetPath).orderBy($"tickerPoint.x").show(false)
    "src/test/scala/org/lamastex/spark/trendcalculus/cleanTmp.sh" !!
  }
}
 

