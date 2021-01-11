package org.lamastex.spark.trendcalculus

import org.scalatest._
import matchers.should._

import scala.io.Source

class ForeignExchangeTest extends SparkSpec with Matchers {
    
    // Test that Foreign Exchange parser works as expected
    sparkTest("Foreign Exchange parser") { spark => 
        import spark.implicits._
        val fxDS = Source.fromInputStream(this.getClass.getResourceAsStream("fx_test.csv"), "UTF-8").getLines().toSeq.toDS().map(Parsers.parseFX)
        fxDS.show
        assert(fxDS.count == 100)
    }

    // Test that we can do Trend Calculus using the Foreign Exchange parser
    sparkTest("Foreign Exchange Trend Calculus") { spark =>
        import spark.implicits._

        import org.lamastex.spark.trendcalculus._

        val fxDF = Source
            .fromInputStream(
                this
                .getClass
                .getResourceAsStream("fx_test.csv"), "UTF-8"
            )
            .getLines()
            .toSeq
            .toDS()
            .map(Parsers.parseFX)
            .toDF
            .select("time","open")

        val trendDF = fxDF.rdd.map(r => ("DUMMY", Point(r.getAs[java.sql.Timestamp]("time").getTime, r.getAs[Double]("open")))).groupByKey().mapValues(it => {
            val series = it.toArray.sortBy(_.x)
            SeriesUtils.completeSeries(series, Frequency.MINUTE, FillingStrategy.LOCF)
        }).flatMap({ case ((s), series) =>
            new TrendCalculus(series, Frequency.HOUR)
                .getTrends
                .filter { trend =>
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
        assert(trendDF.count == 1)

        /* 
        +-----+-------------------+-----+
        |trend|                  x|    y|
        +-----+-------------------+-----+
        | HIGH|2016-01-03 19:27:00|38.48|
        |  LOW|2016-01-03 21:17:00|37.84|
        | HIGH|2016-01-03 23:08:00|38.23|
        |  LOW|2016-01-04 04:17:00|37.09|
        | HIGH|2016-01-04 06:19:00|38.16|
        |  LOW|2016-01-04 06:43:00|37.74|
        | HIGH|2016-01-04 10:02:00|38.96|
        |  LOW|2016-01-04 11:46:00|36.81|
        | HIGH|2016-01-04 13:06:00|37.27|
        |  LOW|2016-01-04 14:21:00|36.87|
        | HIGH|2016-01-04 16:34:00|37.42|
        |  LOW|2016-01-04 17:45:00|37.33|
        | HIGH|2016-01-04 17:52:00|37.38|
        |  LOW|2016-01-04 18:59:00|37.35|
        | HIGH|2016-01-04 19:00:00|37.35|
        |  LOW|2016-01-04 20:18:00| 37.3|
        | HIGH|2016-01-04 20:59:00|37.53|
        |  LOW|2016-01-04 21:51:00|37.41|
        | HIGH|2016-01-04 22:08:00|37.56|
        |  LOW|2016-01-04 23:31:00|37.45|
        +-----+-------------------+-----+
        only showing top 20 rows
        */
    }
}

