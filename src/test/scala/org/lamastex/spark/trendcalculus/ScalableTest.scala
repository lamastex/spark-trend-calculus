package org.lamastex.trendcalculus

import org.scalatest.Matchers

import scala.io.Source

class ScalableTest extends SparkSpec with Matchers {

  // Test that Brent Trend Calculus works as expected
  sparkTest("Scalable Trend Calculus") { spark => 

    import org.apache.spark.sql.functions._
    import spark.implicits._

    import org.lamastex.trendcalculus.DateUtils.Frequency
    import org.lamastex.trendcalculus.SeriesUtils.FillingStrategy
    import org.lamastex.trendcalculus._

    val filePathRoot: String = "file:///root/GIT/lamastex/spark-trend-calculus/src/test/resources/org/lamastex/trendcalculus/"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePathRoot+"brent.csv").filter(year(col("DATE")) >= 2015)

    val toMilliUDF = udf( { time: java.sql.Timestamp => time.getTime() } )
    val milliDF = df.withColumn("TIME", toMilliUDF($"DATE")).drop("DATE")
    val pointDS = milliDF.select($"TIME".as("x"), $"VALUE".as("y")).as[Point]

    pointDS.show

    val windowSize = 3

    val tc = new TrendCalculus2(pointDS, windowSize, spark)

    val reversalTS = tc.getReversals
    reversalTS.show(false)
  }
}

