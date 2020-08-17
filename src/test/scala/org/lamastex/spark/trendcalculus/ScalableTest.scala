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
    val pointDS = df.withColumn("ticker", lit("brent")).select($"ticker", $"DATE" as "x", $"VALUE" as "y").as[TickerPoint]

    pointDS.show

    val windowSize = 2
    val n = 5

    val tc = new TrendCalculus2(pointDS, windowSize, spark)
    val tc2 = new TrendCalculus2(pointDS, windowSize, spark, false)

    val reversalTS = tc.reversals
    val reversalTS2 = tc2.reversals
    reversalTS.show(false)
    reversalTS2.show(false)

    val nReversalTSs = tc.nReversalsJoinedWithMaxRev(n)
    nReversalTSs.show(false)
  }
}

