package org.lamastex.trendcalculus

import org.scalatest.Matchers

import scala.io.Source

class YFinanceTest extends SparkSpec with Matchers {
    
    // Test that yfinance parser works as expected
    sparkTest("yfinance parser") { spark => 
        import spark.implicits._
        val minuteDS = Source.fromInputStream(this.getClass.getResourceAsStream("yfin_test_1m.csv"), "UTF-8").getLines().toSeq.tail.toDS().map(YFinance.parseYF)
        val hourDS = Source.fromInputStream(this.getClass.getResourceAsStream("yfin_test_1h.csv"), "UTF-8").getLines().toSeq.tail.toDS().map(YFinance.parseYF)
        val dayDS = Source.fromInputStream(this.getClass.getResourceAsStream("yfin_test_1d.csv"), "UTF-8").getLines().toSeq.tail.toDS().map(YFinance.parseYF)
        val monthDS = Source.fromInputStream(this.getClass.getResourceAsStream("yfin_test_1mo.csv"), "UTF-8").getLines().toSeq.tail.toDS().map(YFinance.parseYF)
        minuteDS.show
        hourDS.show
        dayDS.show
        monthDS.show
    }
}

