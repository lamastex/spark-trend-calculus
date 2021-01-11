package org.lamastex.spark.trendcalculus

import org.scalatest._
import matchers.should._
import scala.io.Source
import org.lamastex.spark.trendcalculus._

class YFinanceTest extends SparkSpec with Matchers {
    
    // Test that yfinance parser works as expected
    sparkTest("yfinance parser") { spark => 
        import spark.implicits._
        val filePathRoot: String = "src/test/resources/org/lamastex/spark/trendcalculus/"
        val minuteDS = spark.read.yfin(filePathRoot + "yfin_test_1m.csv")
        val hourDS = spark.read.yfin(filePathRoot + "yfin_test_60m.csv")
        val dayDS = spark.read.yfin(filePathRoot + "yfin_test_1d.csv")
        val monthDS = spark.read.yfin(filePathRoot + "yfin_test_1mo.csv")
        minuteDS.show
        hourDS.show
        dayDS.show
        monthDS.show
    }
}

