package org.lamastex.spark.trendcalculus

import org.scalatest.Matchers

import scala.io.Source

class ScalableTest extends SparkSpec with Matchers {

  // Test that Brent Trend Calculus works as expected
  sparkTest("Scalable Trend Calculus") { spark => 

    import org.apache.spark.sql.functions._
    import spark.implicits._

    import org.lamastex.spark.trendcalculus._

    val filePathRoot: String = "src/test/resources/org/lamastex/spark/trendcalculus/"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePathRoot+"brent.csv").filter(year(col("DATE")) >= 2015)

    // val toMilliUDF = udf( { time: java.sql.Timestamp => time.getTime() } )
    // val milliDF = df.withColumn("TIME", toMilliUDF($"DATE")).drop("DATE")
    // val pointDS = milliDF.select($"TIME".as("x"), $"VALUE".as("y")).as[Point]
    val pointDS = df.select($"DATE" as "x", $"VALUE" as "y").as[TimePoint]

    pointDS.show

    val windowSize = 20

    val tc = new TrendCalculus2(pointDS, windowSize, spark)
    val tc2 = new TrendCalculus2(pointDS, windowSize, spark, false)

    val reversalTS = tc.getReversals
    val reversalTS2 = tc2.getReversals
    reversalTS.show(false)
    reversalTS2.show(false)
  }
}

