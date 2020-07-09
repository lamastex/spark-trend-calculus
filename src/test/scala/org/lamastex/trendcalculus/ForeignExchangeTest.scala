package org.lamastex.trendcalculus

import org.scalatest.Matchers

import scala.io.Source

class ForeignExchangeTest extends SparkSpec with Matchers {
    
/*     // Test that Foreign Exchange parser works as expected
    sparkTest("Foreign Exchange parser") { spark => 
        import spark.implicits._
        val fxDS = Source.fromInputStream(this.getClass.getResourceAsStream("DAT_ASCII_BCOUSD_M1_2016.csv"), "UTF-8").getLines().toSeq.toDS().map(ForeignExchange.parseFX)
        fxDS.show
        assert(fxDS.count == 296922)
    } */

    // Test that we can do Trend Calculus using the Foreign Exchange parser
    sparkTest("Foreign Exchange Trend Calculus") { spark =>
        import spark.implicits._

        import org.lamastex.trendcalculus.DateUtils.Frequency
        import org.lamastex.trendcalculus.SeriesUtils.FillingStrategy
        import org.lamastex.trendcalculus._

        val fxDF = Source
            .fromInputStream(
                this
                .getClass
                .getResourceAsStream("DAT_ASCII_BCOUSD_M1_2016.csv"), "UTF-8"
            )
            .getLines()
            .toSeq
            .toDS()
            .map(ForeignExchange.parseFX)
            .toDF
            .select("time","open")
            //.limit(106870)
            .limit(102000)

        /* fxDF.show
        println(fxDF.count) */

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
        println(trendDF.count)
    }
}

