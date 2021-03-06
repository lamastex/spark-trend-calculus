package org.lamastex.spark

import org.apache.spark.sql._
import java.sql.Timestamp
import java.text.SimpleDateFormat

package object trendcalculus {

  case class FXData(
    time: Option[Timestamp] = None,
    open: Option[Double] = None,
    high: Option[Double] = None,
    low: Option[Double] = None,
    close: Option[Double] = None,
    volume: Option[Int] = None
  )

  case class YFData(
    time: Option[Timestamp] = None,
    ticker: Option[String] = None,
    open: Option[Double] = None,
    high: Option[Double] = None,
    low: Option[Double] = None,
    close: Option[Double] = None,
    adjClose: Option[Double] = None,
    volume: Option[Int] = None
  )

  implicit class Finance(dfReader: DataFrameReader) {
    def fx1m(inputPaths: String*): Dataset[FXData] = {
      val ds = dfReader.textFile(inputPaths:_*)
      import ds.sparkSession.implicits._
      ds.map(Parsers.parseFX)
    }

    def fx1m(inputPath: String): Dataset[FXData] = {
     fx1m(Seq(inputPath): _*)
    }

    def yfin(inputPaths: String*): Dataset[YFData] = {
      val ds = dfReader.textFile(inputPaths:_*)
      import ds.sparkSession.implicits._
      ds.map(Parsers.parseYF)
    }

    def yfin(inputPath: String): Dataset[YFData] = {
     yfin(Seq(inputPath): _*)
    }
  }

  case class FrequencyMillisecond(
                                   frequency: Frequency.Value,
                                   milliseconds: Long
                                 )

  case class MonthYear(
                        quarter: Int,
                        half: Int
                      )

  object FillingStrategy extends Enumeration with Serializable {
    val MEAN, LOCF, LINEAR, ZERO = Value
  }

  object AggregateStrategy extends Enumeration with Serializable {
    val MEAN, SUM = Value
  }

  object Frequency extends Enumeration with Serializable {
    val UNKWOWN, MILLI_SECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, HALF_YEAR, YEAR = Value
  }
}