package org.lamastex.spark.trendcalculus

import org.scalatest.Matchers
import scala.io.Source
import org.lamastex.trendcalculus._

class YFinanceTest extends SparkSpec with Matchers {
    
    // Test that yfinance parser works as expected
    sparkTest("yfinance parser") { spark => 
        import spark.implicits._
        val filePathRoot: String = "file:///root/GIT/lamastex/spark-trend-calculus/src/test/resources/org/lamastex/trendcalculus/"
        val minuteDS = spark.read.option("header", true).yfin(filePathRoot + "yfin_test_1m.csv")

        // val minuteDS = Source.fromInputStream(this.getClass.getResourceAsStream("yfin_test_1m.csv"), "UTF-8").getLines().toSeq.tail.toDS().map(FinanceParsers.parseYF)
        // val hourDS = Source.fromInputStream(this.getClass.getResourceAsStream("yfin_test_1h.csv"), "UTF-8").getLines().toSeq.tail.toDS().map(FinanceParsers.parseYF)
        // val dayDS = Source.fromInputStream(this.getClass.getResourceAsStream("yfin_test_1d.csv"), "UTF-8").getLines().toSeq.tail.toDS().map(FinanceParsers.parseYF)
        // val monthDS = Source.fromInputStream(this.getClass.getResourceAsStream("yfin_test_1mo.csv"), "UTF-8").getLines().toSeq.tail.toDS().map(FinanceParsers.parseYF)
        minuteDS.show
        // hourDS.show
        // dayDS.show
        // monthDS.show
    }
}

