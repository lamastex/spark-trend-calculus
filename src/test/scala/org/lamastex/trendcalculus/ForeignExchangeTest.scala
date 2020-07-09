package org.lamastex.trendcalculus

import org.scalatest.Matchers

import scala.io.Source

class ForeignExchangeTest extends SparkSpec with Matchers {
    
    // Test that Foreign Exchange parser works as expected
    sparkTest("Foreign Exchange parser") { spark => 
        import spark.implicits._
        val fxDS = Source.fromInputStream(this.getClass.getResourceAsStream("DAT_ASCII_BCOUSD_M1_2016.csv"), "UTF-8").getLines().toSeq.toDS().map(ForeignExchange.parseFX)
        fxDS.show
    }
}

