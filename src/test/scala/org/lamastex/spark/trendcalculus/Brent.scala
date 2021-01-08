package org.lamastex.spark.trendcalculus

import org.scalatest._
import matchers.should._

import scala.io.Source

class BrentTest extends SparkSpec with Matchers {

  // Test that Brent Trend Calculus works as expected
  sparkTest("Brent Trend Calculus") { spark => 

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    import org.lamastex.spark.trendcalculus._

    def stringToDateLong(s: String): Long = {
      new java.text.SimpleDateFormat("yyyy-MM-dd").parse(s).getTime
    }

    val dateUDF = udf((s: String) => new java.sql.Date(new java.text.SimpleDateFormat("yyyy-MM-dd").parse(s).getTime))
    val valueUDF = udf((s: String) => s.toDouble)

    // Only keep brent with data we know could match ours
    val filePathRoot: String = "src/test/resources/org/lamastex/spark/trendcalculus/"
    val DF = spark.read.option("header", "true").option("inferSchema", "true").csv(filePathRoot+"brent.csv").filter(year(col("DATE")) >= 2015)
    DF.show
    DF.createOrReplaceTempView("brent")

    assert(DF.count == 532)

    val trendDF = DF.rdd.map(r => ("DUMMY", Point(stringToDateLong(r.getAs[String]("DATE")), r.getAs[Double]("VALUE")))).groupByKey().mapValues(it => {
      val series = it.toArray.sortBy(_.x)
      SeriesUtils.completeSeries(series, Frequency.DAY, FillingStrategy.LOCF)
    }).flatMap({ case ((s), series) =>
      new TrendCalculus(series, Frequency.MONTH).getTrends.filter { trend =>
        trend.reversal.isDefined
      }
        .map { trend =>
          (trend.trend.toString, trend.reversal.get.x, trend.reversal.get.y)
        }
        .map { case (trend, x, y) =>
          (trend, new java.sql.Timestamp(x), y)
        }
    }).toDF("trend", "x", "y")

    trendDF.show
    assert(trendDF.count == 11)

      /*
    +-----+--------------------+-----+
    |trend|                   x|    y|
    +-----+--------------------+-----+
    |  LOW|2015-01-13 00:00:...|45.13|
    | HIGH|2015-05-13 00:00:...|66.33|
    |  LOW|2015-08-24 00:00:...|41.59|
    | HIGH|2015-10-08 00:00:...|52.13|
    |  LOW|2016-01-20 00:00:...|26.01|
    | HIGH|2016-06-08 00:00:...|50.73|
    |  LOW|2016-08-02 00:00:...| 40.0|
    | HIGH|2016-08-26 00:00:...|49.66|
    |  LOW|2016-09-27 00:00:...|44.95|
    | HIGH|2016-10-19 00:00:...|51.85|
    |  LOW|2016-11-13 00:00:...|41.61|
    +-----+--------------------+-----+
      */

    val trendUDF = udf((t: String) => if (t == null) "NEUTRAL" else t)
    val result = trendDF.join(DF, trendDF("x") === DF("DATE"), "right_outer").withColumn("TREND", trendUDF(col("trend"))).select("DATE", "VALUE", "TREND") //.createOrReplaceTempView("trends")
    result.show
    assert(result.count == 532)
    
  }
}

